require 'test/unit'
require File.dirname(__FILE__) + '/../lib/memcache/server'
require 'memcache_server_test_helper'

class MemcacheServerTest < Test::Unit::TestCase
  include MemcacheServerTestHelper

  def setup
    @memcache = Memcache::Server.new(:host => 'localhost')
    @memcache.flush_all
  end
end
