require 'test/unit'
require File.dirname(__FILE__) + '/memcache_server_test_helper'

class MemcacheServerTest < Test::Unit::TestCase
  include MemcacheServerTestHelper
  PORT = 11212

  def setup
    start_memcache(PORT)
    @memcache = Memcache::Server.new(:host => 'localhost', :port => PORT)
  end
  
  def teardown
    stop_memcache(PORT)
  end

  def test_gets_and_cas
    m.set('thom', 'hartmann')
    
    value, cas_unique = m.gets('thom')    
    assert_equal 'hartmann', value
    m.cas('thom', 'thompson', cas_unique)
    assert_equal 'thompson', m.get('thom')

    value, cas_unique = m.gets('thom')
    m.delete('thom')
    assert_nil m.cas('thom', 'hartson', cas_unique)
    assert_equal nil, m.get('thom')

    m.add('thom', 'hartmann')
    value, cas_unique = m.gets('thom')
    m.set('thom', 'foo')
    assert_nil m.cas('thom', 'hartson', cas_unique)
    assert_equal 'foo', m.get('thom')
  end
end
