require 'zlib'

$:.unshift(File.dirname(__FILE__))
require 'memcache/base'
require 'memcache/server'
require 'memcache/local_server'
begin
  require 'memcache/native_server'
rescue LoadError => e
  puts "memcache is not using native bindings. for faster performance, compile extensions by hand or install as a local gem."
end
require 'memcache/segmented'

class Memcache
  DEFAULT_EXPIRY  = 0
  LOCK_TIMEOUT    = 5
  WRITE_LOCK_WAIT = 1

  attr_reader :default_expiry, :namespace, :servers, :backup

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
    @default_expiry   = opts[:default_expiry] || DEFAULT_EXPIRY
    @backup           = opts[:backup] # for multi-level caches
    @hash_with_prefix = opts[:hash_with_prefix].nil? ? true : opts[:hash_with_prefix]

    if opts[:native]
      native_opts = opts.clone
      native_opts[:servers] = (opts[:servers] || [ opts[:server] ]).collect do |server|
        server.is_a?(Hash) ? "#{server[:host]}:#{server[:port]}:#{server[:weight]}" : server
      end
      native_opts[:hash] ||= :crc unless native_opts[:ketama] or native_opts[:ketama_wieghted]
      native_opts[:hash_with_prefix] = @hash_with_prefix

      server_class = opts[:segment_large_values] ? SegmentedNativeServer : NativeServer
      @servers = [server_class.new(native_opts)]
    else
      raise "only CRC hashing is supported unless :native => true" if opts[:hash] and opts[:hash] != :crc

      server_class = opts[:segment_large_values] ? SegmentedServer : Server
      @servers = (opts[:servers] || [ opts[:server] ]).collect do |server|
        case server
        when Hash
          server = server_class.new(opts.merge(server))
        when String
          host, port = server.split(':')
          server = server_class.new(opts.merge(:host => host, :port => port))
        when Class
          server = server.new
        when :local
          server = Memcache::LocalServer.new
        end
        server
      end
    end

    @server = @servers.first if @servers.size == 1 and @backup.nil?
    self.namespace = opts[:namespace] if opts[:namespace]
  end

  def clone
    self.class.new(
      :default_expiry => default_expiry,
      :namespace      => namespace,
      :servers        => servers.collect {|s| s.clone}
    )
  end

  def inspect
    "<Memcache: %d servers, ns: %p>" % [@servers.length, namespace]
  end

  def namespace=(namespace)
    @namespace = namespace
    prefix = namespace ? "#{namespace}:" : nil
    servers.each do |server|
      server.prefix = prefix
    end
    backup.namespace = @namespace if backup
    @namespace
  end

  def in_namespace(namespace)
    # Temporarily change the namespace for convenience.
    begin
      old_namespace  = self.namespace
      self.namespace = old_namespace ? "#{old_namespace}:#{namespace}" : namespace
      yield
    ensure
      self.namespace = old_namespace
    end
  end

  def get(keys, opts = {})
    raise 'opts must be hash' unless opts.instance_of?(Hash)

    if keys.instance_of?(Array)
      keys = keys.collect {|key| key.to_s}
      multi_get(keys, opts)
    else
      key = keys.to_s
      if opts[:expiry]
        value = server(key).gets(key)
        cas(key, value, :raw => true, :cas => value.memcache_cas, :expiry => opts[:expiry]) if value
      else
        value = server(key).get(key, opts[:cas])
      end

      return backup.get(key, opts) if backup and value.nil?
      opts[:raw] ? value : unmarshal(value, key)
    end
  end

  def read(key, opts = nil)
    opts ||= {}
    get(key, opts.merge(:raw => true))
  end

  def read_multi(*keys)
    get(keys)
  end

  def set(key, value, opts = {})
    opts = compatible_opts(opts)
    key  = key.to_s
    backup.set(key, value, opts) if backup

    expiry = opts[:expiry] || default_expiry
    flags  = opts[:flags]  || 0
    data   = marshal(value, opts)
    server(key).set(key, data, expiry, flags)
    value
  end

  def write(key, value, opts = nil)
    opts ||= {}
    set(key, value, opts.merge(:raw => true))
  end

  def add(key, value, opts = {})
    opts = compatible_opts(opts)
    key  = key.to_s
    backup.add(key, value, opts) if backup

    expiry = opts[:expiry] || default_expiry
    flags  = opts[:flags]  || 0
    data   = marshal(value, opts)
    server(key).add(key, data, expiry, flags) && value
  end

  def replace(key, value, opts = {})
    opts = compatible_opts(opts)
    key  = key.to_s
    backup.replace(key, value, opts) if backup

    expiry = opts[:expiry] || default_expiry
    flags  = opts[:flags]  || 0
    data   = marshal(value, opts)
    server(key).replace(key, data, expiry, flags) && value
  end

  def cas(key, value, opts)
    raise 'opts must be hash' unless opts.instance_of?(Hash)
    key = key.to_s
    backup.cas(key, value, opts) if backup

    expiry = opts[:expiry] || default_expiry
    flags  = opts[:flags]  || 0
    data   = marshal(value, opts)
    server(key).cas(key, data, opts[:cas], expiry, flags) && value
  end

  def append(key, value)
    key = key.to_s
    backup.append(key, value) if backup
    server(key).append(key, value)
  end

  def prepend(key, value)
    key = key.to_s
    backup.prepend(key, value) if backup
    server(key).prepend(key, value)
  end

  def count(key)
    value = get(key, :raw => true)
    value.to_i if value
  end

  def incr(key, amount = 1)
    key = key.to_s
    backup.incr(key, amount) if backup
    server(key).incr(key, amount)
  end

  def decr(key, amount = 1)
    key = key.to_s
    backup.decr(key, amount) if backup
    server(key).decr(key, amount)
  end

  def update(key, opts = {})
    key   = key.to_s
    value = get(key, :cas => true)
    if value
      cas(key, yield(value), opts.merge!(:cas => value.memcache_cas))
    else
      add(key, yield(value), opts)
    end
  end

  def get_or_add(key, *args)
    # Pseudo-atomic get and update.
    key = key.to_s
    if block_given?
      opts = args[0] || {}
      get(key) || add(key, yield, opts) || get(key)
    else
      opts = args[1] || {}
      get(key) || add(key, args[0], opts) || get(key)
    end
  end

  def get_or_set(key, *args)
    key = key.to_s
    if block_given?
      opts = args[0] || {}
      get(key) || set(key, yield, opts)
    else
      opts = args[1] || {}
      get(key) || set(key, args[0], opts)
    end
  end

  def add_or_get(key, value, opts = {})
    # Try to add, but if that fails, get the existing value.
    add(key, value, opts) || get(key)
  end

  def get_some(keys, opts = {})
    keys = keys.collect {|key| key.to_s}
    records = opts[:disable] ? {} : self.multi_get(keys, opts)
    if opts[:validation]
      records.delete_if do |key, value|
        not opts[:validation].call(key, value)
      end
    end

    keys_to_fetch = keys - records.keys
    method = opts[:overwrite] ? :set : :add
    if keys_to_fetch.any?
      yield(keys_to_fetch).each do |key, value|
        begin
          self.send(method, key, value, opts) unless opts[:disable] or opts[:disable_write]
        rescue Memcache::Error => e
          raise if opts[:strict_write]
          $stderr.puts "Memcache error in get_some: #{e.class} #{e.to_s} on key '#{key}' while storing value: #{value}"
        end
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
    key = key.to_s
    backup.delete(key) if backup
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
    opts.instance_of?(Hash) ? opts : {:expiry => opts}
  end

  def multi_get(keys, opts = {})
    return {} if keys.empty?

    results = {}
    fetch_results = lambda do |server, keys|
      server.get(keys, opts[:cas]).each do |key, value|
        results[key] = opts[:raw] ? value : unmarshal(value, key)
      end
    end

    if @server
      fetch_results.call(@server, keys)
    else
      keys_by_server = Hash.new { |h,k| h[k] = [] }

      # Store keys by servers.
      keys.each do |key|
        keys_by_server[server(key)] << key
      end

      # Fetch and combine the results.
      keys_by_server.each do |server, server_keys|
        fetch_results.call(server, server_keys)
      end
    end

    if backup
      missing_keys = keys - results.keys
      results.merge!(backup.get(missing_keys, opts)) if missing_keys.any?
    end
    results
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
    $stderr.puts "Memcache read error: #{e.class} #{e.to_s} on key '#{key}' while unmarshalling value: #{value}"
    nil
  end

  def server(key)
    return @server if @server

    key = "#{namespace}:#{key}" if @hash_with_prefix and namespace
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
