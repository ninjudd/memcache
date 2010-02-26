class Memcache
  class NullServer < Base
    def name
      "null"
    end

    def flush_all(delay = nil)
    end

    def get(keys)
      keys.kind_of?(Array) ? {} : nil
    end

    def incr(key, amount = nil)
      nil
    end

    def delete(key, expiry = nil)
      nil
    end

    def set(key, value, expiry = nil)
      nil
    end

    def add(key, value, expiry = nil)
      nil
    end
  end
end
