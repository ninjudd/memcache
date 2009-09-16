require File.dirname(__FILE__) + '/test_helper'

module MemcacheServerTestHelper
  def m
    @memcache
  end

  def test_set_and_get
    m.set(2, 'foo', 0)
    
    assert_equal 'foo', m.get('2')
    assert_equal 'foo', m.get('2')

    m.set(2, 'bar', 0)

    assert_equal 'bar', m.get('2')
    assert_equal 'bar', m.get('2')
  end

  def test_expiry
    m.set('foo', 'foo', 1)
    assert_equal 'foo', m.get('foo')

    m.add('bar', 'bar', 1)
    assert_equal 'bar', m.get('bar')

    m.set('baz', '')
    m.replace('baz', 'baz', 1)
    assert_equal 'baz', m.get('baz')

    sleep 1.5

    assert_equal nil, m.get('foo')
    assert_equal nil, m.get('bar')
    assert_equal nil, m.get('baz')
  end

  def test_add_and_replace
    # Replace should do nothing if key doesn't exist.
    m.replace('foo', 'bar')
    assert_equal nil, m.get('foo')

    # Add should only work if key doesn't exist.
    m.add('foo', 'foo')
    assert_equal 'foo', m.get('foo')
    assert_equal nil,   m.add('foo', 'bar')
    assert_equal 'foo', m.get('foo')
    
    # Replace should only work if key doesn't exist.
    m.replace('foo', 'bar')
    assert_equal 'bar', m.get('foo')
  end

  def test_append_and_prepend
    m.append('foo', 'bar')
    assert_equal nil, m.get('foo')

    m.set('foo', 'foo')
    m.append('foo', 'bar')
    assert_equal 'foobar', m.get('foo')

    m.prepend('foo', 'baz')
    assert_equal 'bazfoobar', m.get('foo')
  end

  def test_incr
    # incr does nothing if value doesn't exist
    m.incr('foo')
    assert_equal nil, m.get('foo')

    m.decr('foo', 1)
    assert_equal nil, m.get('foo')

    m.set('foo', '0')
    m.incr('foo')
    assert_equal '1', m.get('foo')

    m.incr('foo', 52)
    assert_equal '53', m.get('foo')

    m.decr('foo', 43)
    assert_equal '10', m.get('foo')

    # Cannot go below zero.
    m.decr('foo', 100)
    assert_equal '0', m.get('foo').strip
  end

  def test_multi_get
    m.set(2, '1,2,3')
    m.set(3, '4,5')
    
    expected = { '2' => '1,2,3', '3' => '4,5' }
    assert_equal expected, m.get([2,3])
  end
  
  def test_delete
    m.set(2, '1,2,3')
    
    assert_equal '1,2,3', m.get(2)

    m.delete(2)
    
    assert_equal nil, m.get(2)
  end

  def test_flush_all
    m.set(2, 'bar')
    
    assert_equal 'bar', m.get(2)

    m.flush_all
    
    assert_equal nil, m.get(2)
  end
  
  module AdvancedMethods
    def test_flags
      m.set('thom', 'hartmann', 0)
      value = m.gets('thom')
      assert_equal 0, value.memcache_flags
      
      m.set('thom', 'hartmann', 0, 0b11110001)
      value = m.gets('thom')
      assert_equal 0b11110001, value.memcache_flags
      
      value = m.get('thom')
      assert_equal 0b11110001, value.memcache_flags
      
      m.set('thom', 'hartmann', 0, 0b10101010)
      value = m.get('thom')
      assert_equal 0b10101010, value.memcache_flags
    end
    
    def test_gets_and_cas
      m.set('thom', 'hartmann')
      
      value = m.gets('thom')    
      assert_equal 'hartmann', value
      m.cas('thom', 'thompson', value.memcache_cas_unique)
      assert_equal 'thompson', m.get('thom')
      
      value = m.gets('thom')
      m.delete('thom')
      assert_nil m.cas('thom', 'hartson', value.memcache_cas_unique)
      assert_equal nil, m.get('thom')
      
      m.add('thom', 'hartmann')
      value = m.gets('thom')
      m.set('thom', 'foo')
      assert_nil m.cas('thom', 'hartson', value.memcache_cas_unique)
      assert_equal 'foo', m.get('thom')
    end
  end
end
