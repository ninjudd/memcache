require 'test/unit'
require File.dirname(__FILE__) + '/../lib/memcache/base'
require File.dirname(__FILE__) + '/../lib/memcache/null_server'

class MemcacheNullServerTest < Test::Unit::TestCase
  def setup
    @memcache = Memcache::NullServer.new
  end

  def m
    @memcache
  end

  def test_set_and_get
    m.set(2, 'foo', 0)

    assert_equal nil, m.get('2')
    assert_equal nil, m.get('2')
  end

  def test_incr
    m.incr('foo')
    assert_equal nil, m.get('foo')

    m.incr('foo', -1)
    assert_equal nil, m.get('foo')

    m.incr('foo', 52)
    assert_equal nil, m.get('foo')

    m.incr('foo', -43)
    assert_equal nil, m.get('foo')
  end

  def test_multi_get
    m.set(2, '1,2,3')
    m.set(3, '4,5')

    assert_equal Hash.new, m.get([2,3])
  end

  def test_delete
    m.set(2, '1,2,3')

    assert_equal nil, m.get(2)

    m.delete(2)

    assert_equal nil, m.get(2)
  end

  def test_flush_all
    m.set(2, 'bar')

    assert_equal nil, m.get(2)

    m.flush_all

    assert_equal nil, m.get(2)
  end

  def test_expiry
    m.add('test', '1', 1)
    assert_equal nil, m.get('test')
  end

  def test_prefix
    assert_equal "foo", m.prefix = "foo"
    assert_equal "foo", m.prefix

    assert_equal nil, m.prefix = nil
    assert_equal nil, m.prefix
  end
end
