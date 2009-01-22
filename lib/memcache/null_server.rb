class Memcache
  class NullServer
    def name
      "null"
    end

    def flush_all(delay = nil)
    end

    def get(key)
      nil
    end

    def get_multi(keys)
      {}
    end

    def incr(key, amount)
      nil
    end

    def delete(key, expiry)
      nil
    end

    def set(key, value, expiry)
      nil
    end

    def add(key, value, expiry)
      nil
    end
  end
end
