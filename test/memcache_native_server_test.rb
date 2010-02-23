require 'test/unit'
require File.dirname(__FILE__) + '/../lib/memcache/native_server'
require File.dirname(__FILE__) + '/memcache_server_test_helper'

class MemcacheLocalServerTest < Test::Unit::TestCase
  include MemcacheServerTestHelper
  include MemcacheServerTestHelper::AdvancedMethods
  #--------------------------------------------------
  # def m; @memcache; end
  #-------------------------------------------------- 
  
  PORT = 11212
  def setup
    start_memcache(PORT)
    @memcache = Memcache::NativeServer.new(:servers => ["localhost:#{PORT}"])
  end

  def teardown
    stop_memcache(PORT)
  end

  def test_server_down
    m = Memcache::NativeServer.new(:servers => ["localhost:9998"])
  
    assert_equal nil, m.get('foo')
  
    assert_raise(Memcache::Error) do
      m.set('foo', 'foo')
    end
  end

  def test_multiple_servers
    port = 11213
    start_memcache(port)
    m = Memcache::NativeServer.new(:servers => ["localhost:#{PORT}", "localhost:#{port}"])
  
    m.set('1', '1')
    m.set('2', '2')
    m.set('3', '3')
    m.set('4', '4')
  
    assert_equal '1', m.get('1')
    assert_equal '2', m.get('2')
    assert_equal '3', m.get('3')
    assert_equal '4', m.get('4')
    stop_memcache(port)
  end
end
