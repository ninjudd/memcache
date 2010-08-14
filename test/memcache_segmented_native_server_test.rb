require 'test/unit'
require File.dirname(__FILE__) + '/memcache_server_test_helper'
require File.dirname(__FILE__) + '/memcache_segmented_test_helper'

$VERBOSE = nil
Memcache::Segmented.const_set('MAX_SIZE', 3)

class MemcacheSegmentedNativeServerTest < Test::Unit::TestCase
  include MemcacheServerTestHelper
  include MemcacheServerTestHelper::AdvancedMethods
  include MemcacheSegmentedTestHelper
  with_prefixes nil, "foo:", "bar:"

  PORTS = [11212, 11213, 11214]
  def setup
    init_memcache(*PORTS) do
      Memcache::SegmentedNativeServer.new(:servers => PORTS.collect {|p| "localhost:#{p}"})
    end
  end
end
