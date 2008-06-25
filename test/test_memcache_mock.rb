require 'test/unit'
require File.dirname(__FILE__) + '/../lib/memcache_mock'

class TestMemCacheMock < Test::Unit::TestCase
  def test_get_set
    m = MemCacheMock.new
    m.set(2, [1,2,3])
    
    assert_equal [1,2,3], m.get(2)
    assert_equal [1,2,3], m.get('2')
  end

  def test_get_set_with_namespace
    m = MemCacheMock.new
    m.namespace = 'electric'
    m.set(2, [1,2,3])
    
    assert_equal [1,2,3], m.get(2)
    assert_equal [1,2,3], m.get('2')
    
    m.namespace = nil
    assert_equal nil, m.get(2)
  end

  def test_multi_get
    m = MemCacheMock.new
    m.set(2, [1,2,3])
    m.set(3, [4,5])
    
    expected = { '2' => [1,2,3], '3' => [4,5] }
    assert_equal expected, m.get_multi(2,3)
  end
  
  def test_multi_get_with_namespace
    m = MemCacheMock.new
    m.namespace = 'mania'
    m.set(2, [1,2,3])
    m.set(3, [4,5])
    
    expected = { '2' => [1,2,3], '3' => [4,5] }
    assert_equal expected, m.get_multi(2,3)
    
    m.namespace = ''
    assert_equal Hash.new, m.get_multi(2,3)
  end

  def test_delete
    m = MemCacheMock.new
    m.set(2, [1,2,3])
    
    assert_equal [1,2,3], m.get(2)

    m.delete(2)
    
    assert_equal nil, m.get(2)
  end

  def test_clear
    m = MemCacheMock.new
    m.set(2, [1,2,3])
    
    assert_equal [1,2,3], m.get(2)

    m.clear
    
    assert_equal nil, m.get(2)
  end
  
  def test_array_operators
    m = MemCacheMock.new
    m[2] = [1,2,3]
    
    assert_equal [1,2,3], m[2]
  end

  def test_array_operators_with_namespace
    m = MemCacheMock.new
    m.namespace = 'fire'
    m[2] = [1,2,3]
    
    assert_equal [1,2,3], m[2]
    
    m.namespace = 'water'
    assert_equal nil, m[2]
  end
  
  def test_expiry
    m = MemCacheMock.new
    m.add('test', 1, 0.1)
    assert_equal 1, m.get('test')
    sleep(0.1)
    assert_equal nil, m.get('test')    
  end
end
