require 'digest/sha1'

class Memcache
  class SegmentedServer < Server
    MAX_SIZE = 1000000 # bytes
    PARTIAL_VALUE = 0x40000000
    
    def get(keys, opts = {})
      return get([keys], opts)[keys.to_s] unless keys.kind_of?(Array)
      return {} if keys.empty?

      results = super
      keys = {}
      keys_to_fetch = []
      results.each do |key, value|
        next unless segmented?(value)
        hash, num = value.split(':')
        keys[key] = []
        num.to_i.times do |i|
          hash_key = "#{hash}:#{i}"
          keys_to_fetch << hash_key
          keys[key]     << hash_key 
        end
      end
      
      parts = super(keys_to_fetch)
      keys.each do |key, hashes|
        value = ''
        hashes.each do |hash_key|
          value << parts[hash_key]
        end
        value.memcache_cas   = results[key].memcache_cas
        value.memcache_flags = results[key].memcache_flags ^ PARTIAL_VALUE
        results[key] = value
      end
      results
    end

    def set(key, value, expiry = 0, flags = 0)
      value, flags = store_segments(key, value, expiry, flags)
      super(key, value, expiry, flags) && value
    end

    def cas(key, value, cas, expiry = 0, flags = 0)
      value, flags = store_segments(key, value, expiry, flags)
      super(key, value, cas, expiry, flags)
    end

    def add(key, value, expiry = 0, flags = 0)
      value, flags = store_segments(key, value, expiry, flags)
      super(key, value, expiry, flags)
    end

    def replace(key, value, expiry = 0, flags = 0)
      value, flags = store_segments(key, value, expiry, flags)
      super(key, value, expiry, flags)
    end

  private

    def segmented?(value)
      value.memcache_flags & PARTIAL_VALUE == PARTIAL_VALUE
    end

    def segment(key, value)
      hash  = Digest::SHA1.hexdigest("#{key}:#{Time.now}:#{rand}")
      parts = {}
      i = 0; offset = 0
      while offset < value.size
        parts["#{hash}:#{i}"] = value[offset, MAX_SIZE]
        offset += MAX_SIZE; i += 1
      end
      master_key = "#{hash}:#{parts.size}"
      [master_key, parts]
    end
    
    def store_segments(key, value, expiry = 0, flags = 0)
      if value and value.size > MAX_SIZE
        master_key, parts = segment(key, value)
        parts.each do |hash, data|
          set(hash, data, expiry)
        end
        [master_key, flags | PARTIAL_VALUE]
      else
        [value, flags]
      end
    end
  end
end
