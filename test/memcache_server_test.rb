require 'test/unit'
require File.dirname(__FILE__) + '/memcache_server_test_helper'

class MemcacheServerTest < Test::Unit::TestCase
  include MemcacheServerTestHelper
  include MemcacheServerTestHelper::AdvancedMethods
  with_prefixes nil, "foo:", "bar:"

  PORT = 11212
  def setup
    init_memcache(PORT) do
      Memcache::Server.new(:host => 'localhost', :port => PORT)
    end
  end

  def test_stats
    m.set('foo', '1')
    m.get('foo')
    m.get('bar')

    stats = m.stats
    assert stats['cmd_get'] > 0
    assert stats['cmd_set'] > 0
    assert stats['curr_items'] > 0
  end

  def test_clone
    m.set('foo', 1)
    c = m.clone

    assert_not_equal m.send(:socket), c.send(:socket)
  end
end
