require 'zlib'

$:.unshift(File.dirname(__FILE__))
require 'memcache/server'

class Memcache
  VERSION = '0.9.0'

  DEFAULT_EXPIRY = 0

  attr_reader :default_expiry, :default_namespace, :servers

  def initialize(opts)
    @readonly          = opts[:readonly]
    @default_expiry    = opts[:default_expiry] || DEFAULT_EXPIRY
    @default_namespace = opts[:namespace]

    @servers = opts[:servers].collect {|s| s.kind_of?(Hash) ? Server.new(s) : s}
  end

  def inspect
    "<Memcache: %d servers, ns: %p, ro: %p>" %
      [@servers.length, namespace, @readonly]
  end

  def namespace
    Thread.current[:memcache_namespace] || default_namespace
  end

  def namespace=(namespace)
    if default_namespace == namespace
      Thread.current[:memcache_namespace] = nil
    else
      Thread.current[:memcache_namespace] = namespace
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

  def get(key, opts = {})
    key   = cache_key(key)
    value = server(key).get(key)
    opts[:raw] ? value : Marshal.load(value)
  end

  def get_multi(*keys)
    opts = keys.last.kind_of?(Hash) ? keys.pop : {}
    
    keys.flatten!
    key_to_input_key = {}
    keys_by_server  = Hash.new { |h,k| h[k] = [] }

    # Store keys by servers. Also store a mapping from cache key to input key.
    keys.each do |input_key|
      key    = cache_key(input_key)
      server = server(key)
      key_to_input_key[key] = input_key 
      keys_by_server[server] << key
    end

    # Fetch and combine the results. Also, map the cache keys back to the input keys.
    results = {}
    keys_by_server.each do |server, keys|
      server.get_multi(keys).each do |key, value|
        input_key = key_to_input_key[key]
        results[input_key] = opts[:raw] ? value : Marshal.load(value)
      end
    end
    results
  end

  def incr(key, amount = 1)
    key = cache_key(key)
    server(key).incr(key, amount)
  end

  def decr(key, amount = 1)
    incr(key, -amount)
  end
  
  def set(key, value, opts = {})
    expiry = opts[:expiry] || default_expiry
    key    = cache_key(key)
    value  = Marshal.dump value unless opts[:raw]
    server(key).set(key, value, expiry)
    value
  end

  def add(key, value, opts = {})
    expiry = opts[:expiry] || default_expiry
    key    = cache_key(key)
    value  = Marshal.dump value unless opts[:raw]
    server(key).add(key, value, expiry)
  end

  def get_or_set(key, opts = {})
    get(key) || set(key, yield, opts)
  end

  def get_some(keys, opts = {})
    expiry = opts[:expiry] || default_expiry

    keys = keys.collect {|key| key.to_s}

    records = {}
    records = self.get_multi(keys) unless opts[:disable]
    if opts[:validation]
      records.delete_if do |key, value|
        not opts[:validation].call(key, value)
      end
    end
    keys_to_fetch = keys - records.keys

    if keys_to_fetch.any?
      yield(keys_to_fetch).each do |key, value|
        self.set(key, value, opts[:expiry]) unless opts[:disable] or opts[:disable_write]
        records[key] = value
      end
    end
    records
  end

  def get_reset_expiry(key, expiry)
    # TODO - fix race condition
    result = get(key)
    set(key, result, expiry) if result
    result
  end


  def delete(key, opts = {})
    key    = cache_key(key)
    server(key).delete(key, opts[:delay])
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
    servers.each do |server|
      server.close
    end
  end

  def stats
    stats = {}
    servers.each do |server|
      stats[server.name] = server.stats
    end
    stats
  end

  alias clear flush_all
  alias [] get

  def []=(key, value)
    set key, value
  end

protected

  def cache_key(key)
    safe_key = key ? key.to_s.gsub(/%/, '%%').gsub(/ /, '%s') : key
    if namespace.nil? then
      safe_key
    else
      "#{namespace}:#{safe_key}"
    end
  end

  def server(key)
    raise ArgumentError, "key too long #{key.inspect}" if key.length > 250
    return servers.first if servers.length == 1

    n = Zlib.crc32(key) % servers.length
    servers[n]
  end

  class Pool
    attr_reader :fallback

    def initialize
      @cache_by_scope = { :default => MemCacheMock.new }
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
  end
  
  def self.pool
    @@cache_pool ||= Pool.new
  end
end
