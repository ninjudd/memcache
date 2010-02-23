require 'zlib'

$:.unshift(File.dirname(__FILE__))
require 'memcache/server'
require 'memcache/local_server'
require 'memcache/segmented_server'
require 'memcache/native_server'

class Memcache
  DEFAULT_EXPIRY  = 0
  LOCK_TIMEOUT    = 5
  WRITE_LOCK_WAIT = 1

  attr_reader :default_expiry, :default_namespace, :servers

  class Error < StandardError; end
  class ConnectionError < Error
    def initialize(e)
      if e.kind_of?(String)
        super
      else
        super("(#{e.class}) #{e.message}")
        set_backtrace(e.backtrace)
      end
    end
  end
  class ServerError < Error; end
  class ClientError < Error; end

  def initialize(opts)
    @default_expiry    = opts[:default_expiry] || DEFAULT_EXPIRY
    @default_namespace = opts[:namespace]
    default_server = opts[:segment_large_values] ? SegmentedServer : Server

    @servers = (opts[:servers] || [ opts[:server] ]).collect do |server|
      case server
      when Hash
        server = default_server.new(opts.merge(server))
      when String
        host, port = server.split(':')
        server = default_server.new(opts.merge(:host => host, :port => port))
      when Class
        server = server.new
      when :local
        server = Memcache::LocalServer.new
      end
      server
    end
  end

  def clone
    self.class.new(
      :default_expiry    => default_expiry,
      :default_namespace => default_namespace,
      :servers           => servers.collect {|s| s.clone}
    )
  end

  def inspect
    "<Memcache: %d servers, ns: %p>" % [@servers.length, namespace]
  end

  def namespace
    @namespace || default_namespace
  end

  def namespace=(namespace)
    if default_namespace == namespace
      @namespace = nil
    else
      @namespace = namespace
    end
  end

  def in_namespace(namespace)
    # Temporarily change the namespace for convenience.
    begin
      old_namespace = self.namespace
      self.namespace = "#{old_namespace}#{namespace}"
      yield
    ensure
      self.namespace = old_namespace
    end
  end

  def get(keys, opts = {})
    raise 'opts must be hash' unless opts.kind_of?(Hash)

    if keys.kind_of?(Array)
      multi_get(keys, opts)
    else
      key = cache_key(keys)

      if opts[:expiry]
        value = server(key).gets(key)
        server(key).cas(key, value, value.memcache_cas, opts[:expiry]) if value
      else
        value = server(key).get(key, opts[:cas])
      end
      opts[:raw] ? value : unmarshal(value, key)
    end
  end

  def read(keys, opts = {})
    get(keys, opts.merge(:raw => true))
  end

  def set(key, value, opts = {})
    opts = compatible_opts(opts)

    expiry = opts[:expiry] || default_expiry
    flags  = opts[:flags]  || 0
    key    = cache_key(key)
    data   = marshal(value, opts)
    server(key).set(key, data, expiry, flags)
    value
  end

  def write(key, value, opts = {})
    set(key, value, opts.merge(:raw => true))
  end

  def add(key, value, opts = {})
    opts = compatible_opts(opts)

    expiry = opts[:expiry] || default_expiry
    flags  = opts[:flags]  || 0
    key    = cache_key(key)
    data   = marshal(value, opts)
    server(key).add(key, data, expiry, flags) && value
  end

  def replace(key, value, opts = {})
    opts = compatible_opts(opts)

    expiry = opts[:expiry] || default_expiry
    flags  = opts[:flags]  || 0
    key    = cache_key(key)
    data   = marshal(value, opts)
    server(key).replace(key, data, expiry, flags) && value
  end

  def cas(key, value, opts = {})
    raise 'opts must be hash' unless opts.kind_of?(Hash)

    expiry = opts[:expiry] || default_expiry
    flags  = opts[:flags]  || 0
    key    = cache_key(key)
    data   = marshal(value, opts)
    server(key).cas(key, data, opts[:cas], expiry, flags) && value
  end

  def append(key, value)
    key = cache_key(key)
    server(key).append(key, value)
  end

  def prepend(key, value)
    key = cache_key(key)
    server(key).prepend(key, value)
  end

  def count(key)
    key = cache_key(key)
    server(key).get(key).to_i
  end

  def incr(key, amount = 1)
    key = cache_key(key)
    server(key).incr(key, amount)
  end

  def decr(key, amount = 1)
    key = cache_key(key)
    server(key).decr(key, amount)
  end

  def update(key, opts = {})
    value = get(key, :cas => true)
    if value
      cas(key, yield(value), opts.merge!(:cas => value.memcache_cas))
    else
      add(key, yield(value), opts)
    end
  end

  def get_or_add(key, *args)
    # Pseudo-atomic get and update.
    if block_given?
      opts = args[0] || {}
      get(key) || add(key, yield, opts) || get(key)
    else
      opts = args[1] || {}
      get(key) || add(key, args[0], opts) || get(key)
    end
  end

  def get_or_set(key, *args)
    if block_given?
      opts = args[0] || {}
      get(key) || set(key, yield, opts)
    else
      opts = args[1] || {}
      get(key) || set(key, args[0], opts)
    end
  end

  def get_some(keys, opts = {})
    keys = keys.collect {|key| key.to_s}

    records = opts[:disable] ? {} : self.get(keys, opts)
    if opts[:validation]
      records.delete_if do |key, value|
        not opts[:validation].call(key, value)
      end
    end

    keys_to_fetch = keys - records.keys
    method = opts[:overwrite] ? :set : :add
    if keys_to_fetch.any?
      yield(keys_to_fetch).each do |key, value|
        self.send(method, key, value, opts) unless opts[:disable] or opts[:disable_write]
        records[key] = value
      end
    end
    records
  end

  def lock(key, opts = {})
    # Returns false if the lock already exists.
    expiry = opts[:expiry] || LOCK_TIMEOUT
    add(lock_key(key), Socket.gethostname, :expiry => expiry, :raw => true)
  end

  def unlock(key)
    delete(lock_key(key))
  end

  def with_lock(key, opts = {})
    until lock(key) do
      return if opts[:ignore]
      sleep(WRITE_LOCK_WAIT) # just wait
    end
    yield
    unlock(key) unless opts[:keep]
  end

  def lock_key(key)
    "lock:#{key}"
  end

  def locked?(key)
    get(lock_key(key), :raw => true)
  end

  def delete(key)
    key = cache_key(key)
    server(key).delete(key)
  end

  def flush_all(opts = {})
    delay    = opts[:delay].to_i
    interval = opts[:interval].to_i

    servers.each do |server|
      server.flush_all(delay)
      delay += interval
    end
  end

  def reset
    servers.each {|server| server.close if server.respond_to?(:close)}
  end

  def stats(field = nil)
    if field
      servers.collect do |server|
        server.stats[field]
      end
    else
      stats = {}
      servers.each do |server|
        stats[server.name] = server.stats
      end
      stats
    end
  end
 
  alias clear flush_all

  def [](key)
    get(key)
  end

  def []=(key, value)
    set(key, value)
  end

  def self.init(yaml_file = nil)
    yaml_file = File.join(Rails.root, 'config', 'memcached.yml')

    if File.exists?(yaml_file)
      yaml = YAML.load_file(yaml_file)
      defaults = (yaml.delete('defaults') || {}).symbolize_keys
      config   = (yaml[Rails.env] || {}).symbolize_keys

      if not config.empty? and not config[:disabled]
        if config[:servers]
          opts = defaults.merge(config.symbolize_keys)
          Object.const_set('CACHE', Memcache.new(opts))
        else
          config.each do |connection, opts|
            opts = defaults.merge(opts.symbolize_keys)
            Memcache.pool[connection] = Memcache.new(opts)
          end
        end
      end
    end
  end

protected

  def compatible_opts(opts)
    # Support passing expiry instead of opts. This may be deprecated in the future.
    opts.kind_of?(Hash) ? opts : {:expiry => opts}
  end

  def multi_get(keys, opts = {})
    return {} if keys.empty?
    
    key_to_input_key = {}
    keys_by_server  = Hash.new { |h,k| h[k] = [] }
    
    # Store keys by servers. Also store a mapping from cache key to input key.
    keys.each do |input_key|
      key    = cache_key(input_key)
      server = server(key)
      key_to_input_key[key] = input_key.to_s
      keys_by_server[server] << key
    end
    
    # Fetch and combine the results. Also, map the cache keys back to the input keys.
    results = {}
    keys_by_server.each do |server, keys|
      server.get(keys, opts[:cas]).each do |key, value|
        input_key = key_to_input_key[key]
        results[input_key] = opts[:raw] ? value : unmarshal(value, key)
      end
    end
    results
  end

  def cache_key(key)
    safe_key = key ? key.to_s.gsub(/%/, '%%').gsub(/ /, '%s') : key
    if namespace.nil? then
      safe_key
    else
      "#{namespace}:#{safe_key}"
    end
  end
  
  def marshal(value, opts = {})
    opts[:raw] ? value : Marshal.dump(value)
  end

  def unmarshal(value, key = nil)
    return value if value.nil?
    
    object = Marshal.load(value)
    object.memcache_flags = value.memcache_flags
    object.memcache_cas   = value.memcache_cas
    object
  rescue Exception => e
    puts "Memcache read error: #{e.class} #{e.to_s} on key '#{key}' while unmarshalling value: #{value}"
    nil
  end

  def server(key)
    raise ArgumentError, "key too long #{key.inspect}" if key.length > 250
    return servers.first if servers.length == 1

    hash = (Zlib.crc32(key) >> 16) & 0x7fff
    servers[hash % servers.length]
  end

  class Pool
    attr_reader :fallback

    def initialize
      @cache_by_scope = {}
      @cache_by_scope[:default] = Memcache.new(:server => Memcache::LocalServer)
      @fallback = :default
    end
    
    def include?(scope)
      @cache_by_scope.include?(scope.to_sym)
    end

    def fallback=(scope)
      @fallback = scope.to_sym
    end

    def [](scope)
      @cache_by_scope[scope.to_sym] || @cache_by_scope[fallback]
    end
    
    def []=(scope, cache)
      @cache_by_scope[scope.to_sym] = cache
    end

    def reset
      @cache_by_scope.values.each {|c| c.reset}
    end
  end
  
  def self.pool
    @@cache_pool ||= Pool.new
  end
end

# Add flags and cas
class Object
  attr_accessor :memcache_flags, :memcache_cas
end
