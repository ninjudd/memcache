class Memcache
  class Base
    attr_accessor :prefix

    def clear
      flush_all
    end

    # Default implementations based on get and set.

    def gets(keys)
      get(keys, true)
    end

    def incr(key, amount = 1)
      value = get(key)
      return unless value
      return unless value =~ /^\d+$/

      value = value.to_i + amount
      value = 0 if value < 0
      set(key, value.to_s)
      value
    end

    def decr(key, amount = 1)
      incr(key, -amount)
    end

    def add(key, value, expiry = 0, flags = 0)
      return nil if get(key)
      set(key, value, expiry)
    end

    def cas(key, value, cas, expiry = 0, flags = 0)
      # No cas implementation yet, just do a set for now.
      set(key, value, expiry, flags)
    end

    def replace(key, value, expiry = 0, flags = 0)
      return nil if get(key).nil?
      set(key, value, expiry)
    end

    def append(key, value)
      existing = get(key)
      return false if existing.nil?
      set(key, existing + value) && true
    end

    def prepend(key, value)
      existing = get(key)
      return false if existing.nil?
      set(key, value + existing) && true
    end

  protected

    def cache_key(key)
      raise Memcache::Error, "length zero key not permitted" if key.length == 0
      key = "#{prefix}#{key}"
      raise Memcache::Error, "key too long #{key.inspect}" if key.length > 250
      key
    end
  end
end
