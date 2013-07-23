require 'digest/sha1'

class Memcache
  module Segmented
    MAX_SIZE = 1000000 # bytes
    PARTIAL_VALUE = 0x40000000

    def get(keys, cas = nil)
      return get([keys], cas)[keys.to_s] unless keys.kind_of?(Array)
      return {} if keys.empty?

      results = super(keys, cas)
      keys = {}
      keys_to_fetch = []
      results.each do |key, result|
        next unless segmented?(result)
        keys[key] = segment_keys(result)
        keys_to_fetch.concat keys[key]
      end

      parts = super(keys_to_fetch)
      keys.each do |key, hashes|
        value = ''
        hashes.each do |hash_key|
          if part = parts[hash_key][:value]
            value << part
          else
            value = nil
            break
          end
        end

        results[key][:value] = value
        results[key][:flags] ^= PARTIAL_VALUE
      end
      results
    end

    def set(key, value, expiry = 0, flags = 0)
      delete(key) do
        hash, flags = store_segments(key, value, expiry, flags)
        super(key, hash, expiry, flags) && value
      end
    end

    def cas(key, value, cas, expiry = 0, flags = 0)
      delete(key) do
        hash, flags = store_segments(key, value, expiry, flags)
        super(key, hash, cas, expiry, flags) && value
      end
    end

    def add(key, value, expiry = 0, flags = 0)
      hash, flags = store_segments(key, value, expiry, flags)
      super(key, hash, expiry, flags) && value
    end

    def replace(key, value, expiry = 0, flags = 0)
      delete(key) do
        hash, flags = store_segments(key, value, expiry, flags)
        super(key, hash, expiry, flags) && value
      end
    end

    def delete(key)
      result = super_get(key)
      enable = block_given? ? yield : super
      if enable and result and segmented?(result)
        segment_keys(result).each {|k| super(k)}
      end
      enable
    end

  private

    def segmented?(result)
      result[:flags] & PARTIAL_VALUE == PARTIAL_VALUE
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
        expiry += 1 unless expiry == 0 # We want the segments to expire slightly after the master key.
        parts.each do |hash, data|
          set(hash, data, expiry)
        end
        [master_key, flags | PARTIAL_VALUE]
      else
        [value, flags]
      end
    end

    def segment_keys(result)
      hash, num = result[:value].split(':')
      (0...num.to_i).collect {|i| "#{hash}:#{i}"}
    end

    def self.included(klass)
      super_get = klass.ancestors[2].instance_method(:get)
      klass.send(:define_method, :super_get) do |key|
        super_get.bind(self).call([key])[key]
      end
    end
  end

  class SegmentedServer < Server
    include Memcache::Segmented
  end

  if defined?(NativeServer)
    class SegmentedNativeServer < NativeServer
      include Memcache::Segmented
    end
  end
end
