require 'test/unit'
require File.dirname(__FILE__) + '/memcache_server_test_helper'

class MemcacheNativeServerTest < Test::Unit::TestCase
  include MemcacheServerTestHelper
  include MemcacheServerTestHelper::AdvancedMethods
  with_prefixes nil, "foo:", "bar:"

  PORTS = [11212, 11213, 11214]
  def setup
    init_memcache(*PORTS) do
      Memcache::NativeServer.new(:servers => PORTS.collect {|p| "localhost:#{p}"})
    end
  end

  def test_server_down
    m = Memcache::NativeServer.new(:servers => ["localhost:9998"])

    assert_equal nil, m.get('foo')
    assert_raise(Memcache::Error) do
      m.set('foo', 'foo')
    end
  end

  def test_close
    m.close

    m.set('foo', 'foo')
    assert_equal 'foo', m.get('foo')
  end
end
