require 'test/unit'
require File.dirname(__FILE__) + '/memcache_server_test_helper'

class MemcacheLocalServerTest < Test::Unit::TestCase
  include MemcacheServerTestHelper
  with_prefixes nil, "foo:", "bar:"

  def setup
    @memcache = Memcache::LocalServer.new
  end
end
