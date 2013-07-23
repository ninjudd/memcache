require File.dirname(__FILE__) + '/test_helper'

module MemcacheServerTestHelper
  def test_prefix
    assert_equal nil, m.prefix = nil
    assert_equal nil, m.prefix

    m.set('1', 'baz', 0)
    m.set('2', 'bar', 0)

    assert_equal nil,   m.prefix
    assert_equal 'foo', m.prefix = 'foo'
    assert_equal 'foo', m.prefix

    m.set('2', 'foo', 0)
    assert_equal nil,   m.get('1')
    assert_equal 'foo', m.get('2')[:value]
    assert_equal({'2' => {:value => 'foo', :flags => 0}}, m.get(['1', '2']))

    assert_equal 'bar:', m.prefix = 'bar:'
    assert_equal 'bar:', m.prefix

    assert_equal nil, m.prefix = nil
    assert_equal nil, m.prefix

    assert_equal 'baz', m.get('1')[:value]
    assert_equal 'bar', m.get('2')[:value]
    assert_equal({'1' => {:value => 'baz', :flags => 0},
                  '2' => {:value => 'bar', :flags => 0}}, m.get(['1', '2']))
  end

  def test_set_and_get
    assert_equal 'foo', m.set('2', 'foo', 0)

    assert_equal 'foo', m.get('2')[:value]
    assert_equal 'foo', m.get('2')[:value]

    assert_equal 'bar', m.set('2', 'bar', 0)

    assert_equal 'bar', m.get('2')[:value]
    assert_equal 'bar', m.get('2')[:value]
  end

  def test_spaces_in_keys
    assert_equal '1', m.set('foo bar', '1', 0)

    assert_equal '1', m.get('foo bar')[:value]
    assert_equal '1', m.get('foo bar')[:value]

    assert_equal '2', m.set('foo bar', '2', 0)

    assert_equal '2', m.get('foo bar')[:value]
    assert_equal '2', m.get('foo bar')[:value]

    assert_equal '8', m.set('foo baz', '8', 0)

    assert_equal({'foo bar' => {:value => '2', :flags => 0},
                  'foo baz' => {:value => '8', :flags => 0}}, m.get(['foo bar','foo baz']))

    assert_equal 'foo', m.set(' ', 'foo', 0)
    assert_equal 'foo', m.get(' ')[:value]
    assert_equal true, m.delete(' ')
  end

  def test_expiry
    assert_equal 'foo', m.set('foo', 'foo', 1)
    assert_equal 'foo', m.get('foo')[:value]

    assert_equal 'bar', m.add('bar', 'bar', 1)
    assert_equal 'bar', m.get('bar')[:value]

    assert_equal '',    m.set('baz', '')
    assert_equal 'baz', m.replace('baz', 'baz', 1)
    assert_equal 'baz', m.get('baz')[:value]

    assert_equal 'bap', m.set('bam', 'bap', (Time.now + 1).to_i)
    assert_equal 'bap', m.get('bam')[:value]

    sleep 2

    assert_equal nil, m.get('foo')
    assert_equal nil, m.get('bar')
    assert_equal nil, m.get('baz')
    assert_equal nil, m.get('bam')
  end

  def test_add_and_replace
    # Replace should do nothing if key doesn't exist.
    assert_equal nil, m.replace('foo', 'bar')
    assert_equal nil, m.get('foo')

    # Add should only work if key doesn't exist.
    assert_equal 'foo', m.add('foo', 'foo')
    assert_equal 'foo', m.get('foo')[:value]
    assert_equal nil,   m.add('foo', 'bar')
    assert_equal 'foo', m.get('foo')[:value]

    # Replace should only work if key doesn't exist.
    assert_equal 'bar', m.replace('foo', 'bar')
    assert_equal 'bar', m.get('foo')[:value]
  end

  def test_append_and_prepend
    assert_equal false, m.append('foo', 'bar')
    assert_equal nil, m.get('foo')

    m.set('foo', 'foo')
    assert_equal true, m.append('foo', 'bar')
    assert_equal 'foobar', m.get('foo')[:value]

    assert_equal true, m.prepend('foo', 'baz')
    assert_equal 'bazfoobar', m.get('foo')[:value]
  end

  def test_incr
    # incr does nothing if value doesn't exist
    assert_equal nil, m.incr('foo')
    assert_equal nil, m.get('foo')

    assert_equal nil, m.decr('foo', 1)
    assert_equal nil, m.get('foo')

    m.set('foo', '0')
    assert_equal 1, m.incr('foo')
    assert_equal '1', m.get('foo')[:value]

    assert_equal 53, m.incr('foo', 52)
    assert_equal '53', m.get('foo')[:value]

    assert_equal 10, m.decr('foo', 43)
    assert_equal '10', m.get('foo')[:value]

    # Cannot go below zero.
    assert_equal 0, m.decr('foo', 100)
    assert_equal '0', m.get('foo')[:value].strip
  end

  def test_multi_get
    m.set('2', '1,2,3')
    m.set('3', '4,5')

    expected = { '2' => {:value => '1,2,3', :flags => 0}, '3' => {:value => '4,5', :flags => 0} }
    assert_equal expected, m.get(['2','3'])
    assert_equal '4,5', m.get(['3'])['3'][:value]
    assert_equal({}, m.get([]))
  end

  def test_delete
    m.set('2', '1,2,3')

    assert_equal '1,2,3', m.get('2')[:value]
    assert_equal true, m.delete('2')
    assert_equal nil, m.get('2')
  end

  def test_flush_all
    m.set('2', 'bar')

    assert_equal 'bar', m.get('2')[:value]

    m.flush_all

    assert_equal nil, m.get('2')
  end

  module AdvancedMethods
    def test_flags
      m.set('thom', 'hartmann', 0)
      assert_equal 0, m.gets('thom')[:flags]

      m.set('thom', 'hartmann', 0, 0b11110001)
      assert_equal 0b11110001, m.gets('thom')[:flags]

      assert_equal 0b11110001, m.get('thom')[:flags]

      m.set('thom', 'hartmann', 0, 0b10101010)
      assert_equal 0b10101010, m.get('thom')[:flags]
    end

    def test_gets_and_cas
      m.set('thom', 'hartmann')

      result = m.gets('thom')
      assert_equal 'hartmann', result[:value]
      assert_equal 'thompson', m.cas('thom', 'thompson', result[:cas])
      assert_equal 'thompson', m.get('thom')[:value]

      result = m.gets('thom')
      m.delete('thom')
      assert_nil m.cas('thom', 'hartson', result[:cas])
      assert_equal nil, m.get('thom')

      m.add('thom', 'hartmann')
      result = m.gets('thom')
      m.set('thom', 'foo')
      assert_nil m.cas('thom', 'hartson', result[:cas])
      assert_equal 'foo', m.get('thom')[:value]
    end
  end
end
