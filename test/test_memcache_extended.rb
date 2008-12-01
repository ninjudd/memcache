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

  def test_in_namespace
    cache = MemCache.new 'localhost:1', :namespace => 'ns'

    threads = []
    100.times do |i|
      threads << Thread.new do
        cache.in_namespace(i.to_s) do
          sleep 0.1
          assert_equal "ns#{i}", cache.namespace
        end
      end
    end
    
    threads.each {|t| t.join}
  end

end
