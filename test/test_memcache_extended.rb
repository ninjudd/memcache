require 'test/unit'
require File.dirname(__FILE__) + '/../lib/memcache_mock'
require File.dirname(__FILE__) + '/../lib/memcache_extended'

class TestGeniMemcache < Test::Unit::TestCase
  # def memcache
  #   MemCache.new(
  #     :servers=>["localhost:11211"],
  #     :ttl=>1800,
  #     :compression=>false,
  #     :readonly=>false,
  #     :debug=>false,
  #     :c_threshold=>10000,
  #     :urlencode=>false
  #   )
  #   #CACHE.servers = memcached_config[:servers]
  # end
  
  def test_get_reset_expiry
    m = MemCacheMock.new
    m.add('rewrite_test', 'quick brown fox', 0.1)
    assert_equal 'quick brown fox', m.get_reset_expiry('rewrite_test', 2)
    sleep(0.1)
    
    assert_equal 'quick brown fox', m.get('rewrite_test')
  end
end
