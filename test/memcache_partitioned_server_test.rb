require 'test/unit'
require File.dirname(__FILE__) + '/memcache_server_test_helper'
require File.dirname(__FILE__) + '/../lib/memcache/partitioned_server'

class MemcachePartitionedServerTest < Test::Unit::TestCase
  include MemcacheServerTestHelper
  include MemcacheServerTestHelper::AdvancedMethods
  PORT = 11212

  def setup
    start_memcache(PORT)
    @memcache = Memcache::PartitionedServer.new(:host => 'localhost', :port => PORT)
  end
  
  def teardown
    stop_memcache(PORT)
  end

  def test_partitioned_values
    Memcache::PartitionedServer.const_override('MAX_SIZE', 5) do
      long = 'a long value that will exceed the limit'
      assert_match /\w{40}:\w+/, m.set('foo', long + 'foo')
      assert_equal long + 'foo', m.get('foo')
      
      assert_match /\w{40}:\w+/, m.set('bar', long + 'bar')
      assert_equal long + 'bar', m.get('bar')
      
      results = m.get(['foo', 'bar'])
      assert_equal long + 'foo', results['foo']
      assert_equal long + 'bar', results['bar']
      
      m.delete('foo')
      assert_equal nil, m.get('foo')
    end
  end
end
