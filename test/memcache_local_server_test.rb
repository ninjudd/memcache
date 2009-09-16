require 'test/unit'
require File.dirname(__FILE__) + '/../lib/memcache/local_server'
require File.dirname(__FILE__) + '/memcache_server_test_helper'

class MemcacheLocalServerTest < Test::Unit::TestCase
  include MemcacheServerTestHelper

  def setup
    @memcache = Memcache::LocalServer.new
  end
end
