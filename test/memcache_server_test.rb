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
end
