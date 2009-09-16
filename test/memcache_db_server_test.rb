require 'test/unit'
require 'rubygems'
require File.dirname(__FILE__) + '/../lib/memcache/db_server'
require File.dirname(__FILE__) + '/memcache_server_test_helper'

class MemcacheDBServerTest < Test::Unit::TestCase
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
    MemcacheDBMigration.table = 'memcache_test'
    MemcacheDBMigration.up
    @memcache = Memcache::DBServer.new(:table => 'memcache_test')
  end

  def teardown
    MemcacheDBMigration.down
  end
end
