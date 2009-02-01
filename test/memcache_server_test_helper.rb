module MemcacheServerTestHelper
  def m
    @memcache
  end

  def test_set_and_get
    m.set(2, 'foo', 0)
    
    assert_equal 'foo', m.get('2')
    assert_equal 'foo', m.get('2')
  end

  def test_multi_get
    m.set(2, '1,2,3')
    m.set(3, '4,5')
    
    expected = { '2' => '1,2,3', '3' => '4,5' }
    assert_equal expected, m.get_multi(2,3)
  end
  
  def test_delete
    m.set(2, '1,2,3')
    
    assert_equal '1,2,3', m.get(2)

    m.delete(2)
    
    assert_equal nil, m.get(2)
  end

  def test_clear
    m.set(2, 'bar')
    
    assert_equal 'bar', m.get(2)

    m.clear
    
    assert_equal nil, m.get(2)
  end
    
  def test_expiry
    m.add('test', 1, 0.1)
    assert_equal 1, m.get('test')
    sleep(0.1)
    assert_equal nil, m.get('test')    
  end
end
