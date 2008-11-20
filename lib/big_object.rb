##
# Extension to the memcached Ruby client library to handle objects that are
# too large to store (1MB by default).

module MemCacheBigObject
  DEFAULT_MAX_SIZE = 1000000 # bytes
  
  class PartitionedObject
    attr_reader :key, :num_parts
    
    PREFIX = '_PObj'
    
    def self.part_key(cache_key, part_num)
      "#{cache_key}_pt#{part_num}"
    end
    
    def self.load(s)
      return nil unless s =~ /^#{PREFIX}/
      (p, num_parts, key) = s.split(':', 3)
      return nil if num_parts.nil? or num_parts.empty? or key.nil? or key.empty?
      new(key, num_parts.to_i)
    end
    
    def initialize(cache_key, num_parts)
      @key = cache_key
      @num_parts = num_parts
    end
    
    def to_s
      "#{PREFIX}:#{num_parts}:#{key}"
    end
    
    def part_keys
      (0...num_parts).collect {|part_num| self.class.part_key(key, part_num) }
    end
    
    def reassemble_parts(parts_results)
      result = ''
      
      num_parts.times do |part_num|
        return nil if parts_results[self.class.part_key(key, part_num)].nil?
        result += parts_results[self.class.part_key(key, part_num)]
      end
      
      result
    end
  end

  def self.included(mod)
    mod.send(:alias_method, :cache_get_without_reassemble, :cache_get)
    mod.send(:alias_method, :cache_get, :cache_get_with_reassemble)
    mod.send(:alias_method, :cache_get_multi_without_reassemble, :cache_get_multi)
    mod.send(:alias_method, :cache_get_multi, :cache_get_multi_with_reassemble)
    mod.send(:alias_method, :cache_store_without_slicing, :cache_store)
    mod.send(:alias_method, :cache_store, :cache_store_with_slicing)
  end
  
  def max_size
    @max_size || DEFAULT_MAX_SIZE
  end
  
  def max_size=(num_bytes)
    @max_size = num_bytes
  end
  
  def partition?(raw_data)
    raw_data.size > max_size
  end
  
  def cache_get_with_reassemble(server, cache_key)
    value = cache_get_without_reassemble(server, cache_key)
    
    partitioned_object = PartitionedObject.load(value)
    
    return value if partitioned_object.nil?
    get_partitioned_objects(partitioned_object, server)[cache_key] 
  end
  
  def cache_get_multi_with_reassemble(server, keys)
    partitioned_objects = {}
    
    values = cache_get_multi_without_reassemble(server, keys)
    values.each do |key, value|
      partitioned_object = PartitionedObject.load(value)
      if not partitioned_object.nil?
        partitioned_objects[key] = partitioned_object
        values.delete(key)
      end
    end
    
    values.merge!( get_partitioned_objects(partitioned_objects.values, server) ) unless partitioned_objects.empty?
    
    values
  end
  
  def cache_store_with_slicing(method, cache_key, value, expiry, server)
    return store_partitions(method, cache_key, value, expiry, server) if partition?(value)
    cache_store_without_slicing(method, cache_key, value, expiry, server)
  end
  
  def store_partitions(method, cache_key, raw_data, expiry, server)
    num_parts = 0
    
    while raw_data and raw_data.size > 0
      cache_store(method, PartitionedObject.part_key(cache_key, num_parts), raw_data[0,max_size], expiry, server)
      raw_data = raw_data[max_size..-1]
      num_parts += 1
    end
    
    cache_store(method, cache_key, PartitionedObject.new(cache_key, num_parts).to_s, expiry, server)
  end
  
  def get_partitioned_objects(key_objects, server)
    key_objects = [*key_objects]
    return {} if key_objects.empty?
    
    all_part_keys = key_objects.collect {|po| po.part_keys } # get_multi will flatten
    
    parts = cache_get_multi(server, all_part_keys.join(' '))
    
    results = {}
    key_objects.each do |po|
      results[po.key] = po.reassemble_parts(parts)
    end
    
    results
  end
end

class MemCache
  include MemCacheBigObject
end

