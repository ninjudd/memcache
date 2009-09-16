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

    keys = (0..200).to_a
    results = m.get(keys)
    assert_equal 100, results.size
    results.each do |key, value|
      assert_equal key.to_i, value
    end
    
    100.times do |i|
      m.set(i.to_s, i.to_s, :raw => true)
      assert_equal i.to_s, m.get(i.to_s, :raw => true)
    end

    results = m.get(keys ,:raw => true)
    assert_equal 100, results.size
    results.each do |key, value|
      assert_equal key, value
    end
  end

  def test_alternate_accessors
    m['baz'] = 24
    assert_equal 24, m['baz']
  end

  def test_get_or_set
    100.times do |i|
      m.get_or_set("foo#{i}", [i, :foo])
      assert_equal [i, :foo], m["foo#{i}"]

      m.get_or_set("foo#{i}") {raise}
      assert_equal [i, :foo], m["foo#{i}"]    

      # Overwrite if changed.
      m.get_or_set("bar#{i}") do
        m.set("bar#{i}", [i, :foo])
        [i, :bar]
      end
      assert_equal [i, :bar], m["bar#{i}"]
    end
  end

  def test_get_or_add
    100.times do |i|
      m.get_or_add("foo#{i}", [:foo, i])
      assert_equal [:foo, i], m["foo#{i}"]

      m.get_or_add("foo#{i}") {raise}
      assert_equal [:foo, i], m["foo#{i}"]    

      # Don't overwrite if changed.
      m.get_or_add("bar#{i}") do
        m.set("bar#{i}", [:foo, i])
        :bar
      end
      assert_equal [:foo, i], m["bar#{i}"]
    end
  end

  def test_add_and_replace
    100.times do |i|
      m.replace(i.to_s, [:foo, i])
      assert_equal nil, m.get(i.to_s)

      m.add(i.to_s, [:bar, i])
      assert_equal [:bar, i], m.get(i.to_s)

      m.replace(i.to_s, [:foo, i])
      assert_equal [:foo, i], m.get(i.to_s)

      m.add(i.to_s, [:baz, i])
      assert_equal [:foo, i], m.get(i.to_s)

      m.replace(i.to_s, 'blah', :raw => true)
      assert_equal 'blah', m.get(i.to_s, :raw => true)

      m.delete(i.to_s)
      assert_equal nil, m.get(i.to_s, :raw => true)

      m.add(i.to_s, 'homerun', :raw => true)
      assert_equal 'homerun', m.get(i.to_s, :raw => true)      
    end
  end

  def test_get_some
    100.times do |i|
      i = i * 2
      m.set(i.to_s, i)
      assert_equal i, m.get(i.to_s)
    end

    keys = (0...200).to_a
    results = m.get_some(keys) do |missing_keys|
      assert_equal 100, missing_keys.size
      r = {}
      missing_keys.each do |key|
        r[key] = key.to_i
      end
      r
    end

    assert_equal 200, results.size
    results.each do |key, value|
      assert_equal key.to_i, value
    end
  end

  def test_get_reset_expiry
    m.add('foo', 'quick brown fox', :expiry => 1)
    assert_equal 'quick brown fox', m.get('foo', :expiry => 2)
    sleep(1)    
    assert_equal 'quick brown fox', m.get('foo')
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
