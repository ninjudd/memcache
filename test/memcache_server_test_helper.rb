require File.dirname(__FILE__) + '/test_helper'

module MemcacheServerTestHelper
  def m
    @memcache
  end

  def test_set_and_get
    m.set(2, 'foo', 0)
    
    assert_equal 'foo', m.get('2')
    assert_equal 'foo', m.get('2')
  end

  def test_incr
    # incr does nothing if value doesn't exist
    m.incr('foo')
    assert_equal nil, m.get('foo')

    m.incr('foo', -1)
    assert_equal nil, m.get('foo')

    m.set('foo', '0')
    m.incr('foo')
    assert_equal '1', m.get('foo')

    m.incr('foo', 52)
    assert_equal '53', m.get('foo')

    m.incr('foo', -43)
    assert_equal '10', m.get('foo')

    # Cannot go below zero.
    m.incr('foo', -100)
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
    
  def test_expiry
    m.add('test', '1', 1)
    assert_equal '1', m.get('test')
    sleep(2)
    assert_equal nil, m.get('test')    
  end
end
