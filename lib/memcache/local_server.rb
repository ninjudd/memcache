class Memcache
  class LocalServer
    def initialize
      @data = {}
      @expiry = {}
    end

    def name
      "local:#{hash}"
    end

    def stats
      { # curr_items may include items that have expired.
        'curr_items'   => @data.size,
        'expiry_count' => @expiry.size,
      }
    end

    def flush_all(delay = 0)
      raise 'flush_all not supported with delay' if delay != 0
      @data.clear
      @expiry.clear
    end

    def gets(keys)
      get(keys, true)
    end

    def get(keys, cas = false)
      if keys.kind_of?(Array)
        hash = {}
        keys.each do |key|
          key = key.to_s
          val = get(key)
          hash[key] = val if val
        end
        hash
      else
        key = keys.to_s
        if @expiry[key] and Time.now > @expiry[key]
          @data[key]   = nil
          @expiry[key] = nil
        end
        @data[key]
      end
    end

    def incr(key, amount = 1)
      key = key.to_s
      value = get(key)
      return unless value
      return unless value =~ /^\d+$/

      value = value.to_i + amount
      value = 0 if value < 0
      @data[key] = value.to_s
      value
    end

    def decr(key, amount = 1)
      incr(key, -amount)
    end

    def delete(key)
      @data.delete(key.to_s)
    end

    def set(key, value, expiry = 0, flags = 0)
      key = key.to_s
      @data[key] = value
      if expiry.kind_of?(Time)
        @expiry[key] = expiry
      else  
        expiry = expiry.to_i
        @expiry[key] = expiry == 0 ? nil : Time.now + expiry
      end
      value
    end

    def cas(key, value, cas, expiry = 0, flags = 0)
      # No cas implementation yet, just do a set for now.
      set(key, value, expiry, flags)
    end

    def add(key, value, expiry = 0, flags = 0)
      return nil if get(key)
      set(key, value, expiry)
    end

    def replace(key, value, expiry = 0, flags = 0)
      return nil if get(key).nil?
      set(key, value, expiry)
    end

    def append(key, value)
      existing = get(key)
      return nil if existing.nil?
      set(key, existing + value)
    end

    def prepend(key, value)
      existing = get(key)
      return nil if existing.nil?
      set(key, value + existing)
    end
  end
end
