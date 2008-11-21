# Need to override entire class with Justin's changes
require File.dirname(__FILE__) + '/memcache'

module MemCacheExtensions
  LOCK_TIMEOUT    = 5     if not defined? LOCK_TIMEOUT
  WRITE_LOCK_WAIT = 0.001 if not defined? WRITE_LOCK_WAIT

  def get_some(keys, opts = {})
    opts[:expiry] ||= default_expiry
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
      yield(keys_to_fetch).each do |key, data_item|
        self.set(key, data_item, opts[:expiry]) unless opts[:disable] or opts[:disable_write]
        records[key] = data_item
      end
    end
    records
  end

  def in_namespace(namespace)
    begin
      # Temporarily change the namespace for convenience.
      ns = self.namespace
      self.instance_variable_set(:@namespace, "#{ns}#{namespace}")      
      yield
    ensure
      self.instance_variable_set(:@namespace, ns)
    end
  end

  def get_or_set(key)
    get(key) || begin
      value = yield
      set(key, value)
      value
    end
  end

  def get_reset_expiry(key, expiry)
    result = get(key)
    set(key, result, expiry) if result
    result
  end

  def lock(key)
    # Returns true if the lock already exists.
    response = add(lock_key(key), true, LOCK_TIMEOUT)
    response.index('STORED') != 0
  end

  def unlock(key)
    response = delete(lock_key(key))
    response.index('DELETED') == 0
  end

  def with_lock(key, flag = nil)
    while lock(key) do
      return if flag == :ignore
      sleep(WRITE_LOCK_WAIT) # just wait
    end
    yield
    unlock(key) unless flag == :keep
  end

  def lock_key(key)
    "lock:#{key}"
  end

  def locked?(key)
    not get(lock_key(key)).nil?
  end

  def set_with_lock(*args)
    with_lock(args.first, :ignore) do
      set(*args)
    end
  end

  def add_with_lock(*args)
    with_lock(args.first, :ignore) do
      add(*args)
    end
  end

  def delete_with_lock(*args)
    # leave a :delete lock around to prevent someone from
    # adding stale data for a little while
    with_lock(args.first, :keep) do
      delete(*args)
    end
  end

  def clear
    flush_all
  end

  ### To support using memcache in testing.
  def empty?; false; end
  ###
end

class MemCache
  include MemCacheExtensions
end

if defined?(MemCacheMock)
  class MemCacheMock
    include MemCacheExtensions
  end
end
