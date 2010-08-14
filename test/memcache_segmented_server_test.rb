require 'test/unit'
require File.dirname(__FILE__) + '/memcache_server_test_helper'
require File.dirname(__FILE__) + '/memcache_segmented_test_helper'

$VERBOSE = nil
Memcache::Segmented.const_set('MAX_SIZE', 3)

class MemcacheSegmentedServerTest < Test::Unit::TestCase
  include MemcacheServerTestHelper
  include MemcacheServerTestHelper::AdvancedMethods
  include MemcacheSegmentedTestHelper

  with_prefixes nil, "foo:", "bar:"

  PORT = 11212
  def setup
    init_memcache(PORT) do
      Memcache::SegmentedServer.new(:host => 'localhost', :port => PORT)
    end
  end
end
