class Memcache
  class LocalServer
    def initialize
      @data = {}
      @expiry = {}
    end

    def name
      "local:#{hash}"
    end

    def flush_all(delay = nil)
      raise 'flush_all not supported with delay' if delay
      @data.clear
      @expiry.clear
    end

    def get(key)
      key = key.to_s

      if @expiry[key] and Time.now > @expiry[key]
        @data[key]   = nil
        @expiry[key] = nil
      end
      @data[key]
    end

    def get_multi(keys)
      hash = {}
      keys.each do |key|
        val = get(key)
        hash[key.to_s] = val if val
      end
      hash
    end

    def incr(key, amount)
      key = key.to_s
      value = get(key)
      return unless value
      return unless value =~ /^\d+$/

      value = value.to_i + amount
      @data[key] = value.to_s
      value
    end

    def delete(key, expiry)
      if expiry
        old_expiry = @expiry[key.to_s] || expiry
        @expiry[key.to_s] = [old_expiry, expiry].min
      else
        @data.delete(key.to_s)
      end
    end

    def set(key, value, expiry)
      key = key.to_s
      @data[key]   = value
      @expiry[key] = Time.now + expiry if expiry and expiry != 0
      value
    end

    def add(key, value, expiry)
      return false if get(key)
      set(key, value, expiry)
    end
  end
end
