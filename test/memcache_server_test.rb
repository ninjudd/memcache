require 'test/unit'
require File.dirname(__FILE__) + '/memcache_server_test_helper'

class MemcacheServerTest < Test::Unit::TestCase
  include MemcacheServerTestHelper
  include MemcacheServerTestHelper::AdvancedMethods
  PORT = 11212

  def setup
    start_memcache(PORT)
    @memcache = Memcache::Server.new(:host => 'localhost', :port => PORT)
  end
  
  def teardown
    stop_memcache(PORT)
  end

  def test_stats
    m.set('foo', '1')
    m.get('foo')
    m.get('bar')

    stats = m.stats
    assert_equal 2, stats['cmd_get'] 
    assert_equal 1, stats['cmd_set']
    assert_equal 1, stats['curr_items']
  end
end
