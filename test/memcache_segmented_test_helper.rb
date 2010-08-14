module MemcacheSegmentedTestHelper
  def test_segmented_delete
    m.set('fav_numbers', '1,2,3,4,5,6,7,8,9,10')

    master_key   = m.send(:super_get, 'fav_numbers')
    segment_keys = m.send(:segment_keys, master_key)

    assert_not_equal '1,2,3,4,5,6,7,8,9,10', master_key
    assert_equal 7, segment_keys.size

    assert_equal '1,2,3,4,5,6,7,8,9,10', m.get('fav_numbers')
    assert_equal true, m.delete('fav_numbers')
    assert_equal nil, m.get('fav_numbers')

    segment_keys.each do |k|
      assert_equal nil, m.get(k)
    end
  end
end
