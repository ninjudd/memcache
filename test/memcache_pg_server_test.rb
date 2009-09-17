require 'test/unit'
require 'rubygems'
require File.dirname(__FILE__) + '/memcache_server_test_helper'
require File.dirname(__FILE__) + '/../lib/memcache/pg_server'

class MemcachePGServerTest < Test::Unit::TestCase
  ActiveRecord::Base.establish_connection(
    :adapter  => "postgresql",
    :host     => "localhost",
    :username => "postgres",
    :password => "",
    :database => "memcache_test"
  )
  ActiveRecord::Migration.verbose = false
  ActiveRecord::Base.connection.client_min_messages = 'panic'

  include MemcacheServerTestHelper

  def setup
    Memcache::Migration.table = 'memcache_test'
    Memcache::Migration.up
    @memcache = Memcache::PGServer.new(:table => 'memcache_test')
  end

  def teardown
    Memcache::Migration.down
  end
end
