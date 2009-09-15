require 'test/unit'
require File.dirname(__FILE__) + '/test_helper'

class MemcacheServerTest < Test::Unit::TestCase
  PORTS = [11212, 11213, 11214, 11215, 11216, 11217]

  def m
    @memcache
  end

  def setup
    start_memcache(*PORTS)
    @memcache = Memcache.new(:servers => PORTS.collect {|p| "localhost:#{p}"})
  end
    
  def teardown
    stop_memcache(*PORTS)
  end

  def test_get_and_set
    100.times do |i|
      m.set(i.to_s, i)
      assert_equal i, m.get(i.to_s)
    end
  end

  def test_alternate_accessors
    m['baz'] = 24
    assert_equal 24, m['baz']
  end

  def test_get_or_set
    
  end

  def test_in_namespace
    threads = []
    10.times do |i|
      m.in_namespace("_#{i}_") do
        10.times do |j|
          m.in_namespace("_#{j}_") do
            assert_equal nil, m.get('foo')
            m.set('foo', 'bar')
            assert_equal 'bar', m.get('foo')
          end
        end
      end
    end
  end

  def test_incr_and_decr
    m.incr('foo', 100)
    assert_equal 100, m.count('foo')

    m.decr('foo', 100)
    assert_equal 0, m.count('foo')

    m.incr('foo', 500)
    assert_equal 500, m.count('foo')

    m.decr('foo', 300)
    assert_equal 200, m.count('foo')

    m.decr('foo', 300)
    assert_equal 0, m.count('foo')
  end
end
