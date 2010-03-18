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
    assert_equal 'foo', m.get('2')
    assert_equal({'2'=>'foo'}, m.get(['1', '2']))

    assert_equal 'bar:', m.prefix = 'bar:'
    assert_equal 'bar:', m.prefix

    assert_equal nil, m.prefix = nil
    assert_equal nil, m.prefix

    assert_equal 'baz', m.get('1')
    assert_equal 'bar', m.get('2')
    assert_equal({'1'=>'baz','2'=>'bar'}, m.get(['1', '2']))
  end

  def test_set_and_get
    assert_equal 'foo', m.set('2', 'foo', 0)

    assert_equal 'foo', m.get('2')
    assert_equal 'foo', m.get('2')

    assert_equal 'bar', m.set('2', 'bar', 0)

    assert_equal 'bar', m.get('2')
    assert_equal 'bar', m.get('2')
  end

  def test_spaces_in_keys
    assert_equal '1', m.set('foo bar', '1', 0)

    assert_equal '1', m.get('foo bar')
    assert_equal '1', m.get('foo bar')

    assert_equal '2', m.set('foo bar', '2', 0)

    assert_equal '2', m.get('foo bar')
    assert_equal '2', m.get('foo bar')

    assert_equal '8', m.set('foo baz', '8', 0)

    expected = { 'foo bar' => '2', 'foo baz' => '8' }
    assert_equal expected, m.get(['foo bar','foo baz'])

    assert_equal 'foo', m.set(' ', 'foo', 0)
    assert_equal 'foo', m.get(' ')
    assert_equal true, m.delete(' ')
  end

  def test_expiry
    assert_equal 'foo', m.set('foo', 'foo', 1)
    assert_equal 'foo', m.get('foo')

    assert_equal 'bar', m.add('bar', 'bar', 1)
    assert_equal 'bar', m.get('bar')

    assert_equal '',    m.set('baz', '')
    assert_equal 'baz', m.replace('baz', 'baz', 1)
    assert_equal 'baz', m.get('baz')

    assert_equal 'bap', m.set('bam', 'bap', (Time.now + 1).to_i)
    assert_equal 'bap', m.get('bam')

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
    assert_equal 'foo', m.get('foo')
    assert_equal nil,   m.add('foo', 'bar')
    assert_equal 'foo', m.get('foo')

    # Replace should only work if key doesn't exist.
    assert_equal 'bar', m.replace('foo', 'bar')
    assert_equal 'bar', m.get('foo')
  end

  def test_append_and_prepend
    assert_equal false, m.append('foo', 'bar')
    assert_equal nil, m.get('foo')

    m.set('foo', 'foo')
    assert_equal true, m.append('foo', 'bar')
    assert_equal 'foobar', m.get('foo')

    assert_equal true, m.prepend('foo', 'baz')
    assert_equal 'bazfoobar', m.get('foo')
  end

  def test_incr
    # incr does nothing if value doesn't exist
    assert_equal nil, m.incr('foo')
    assert_equal nil, m.get('foo')

    assert_equal nil, m.decr('foo', 1)
    assert_equal nil, m.get('foo')

    m.set('foo', '0')
    assert_equal 1, m.incr('foo')
    assert_equal '1', m.get('foo')

    assert_equal 53, m.incr('foo', 52)
    assert_equal '53', m.get('foo')

    assert_equal 10, m.decr('foo', 43)
    assert_equal '10', m.get('foo')

    # Cannot go below zero.
    assert_equal 0, m.decr('foo', 100)
    assert_equal '0', m.get('foo').strip
  end

  def test_multi_get
    m.set('2', '1,2,3')
    m.set('3', '4,5')

    expected = { '2' => '1,2,3', '3' => '4,5' }
    assert_equal expected, m.get(['2','3'])
    assert_equal '4,5', m.get(['3'])['3']
    assert_equal({}, m.get([]))
  end

  def test_delete
    m.set('2', '1,2,3')

    assert_equal '1,2,3', m.get('2')
    assert_equal true, m.delete('2')
    assert_equal nil, m.get('2')
  end

  def test_flush_all
    m.set('2', 'bar')

    assert_equal 'bar', m.get('2')

    m.flush_all

    assert_equal nil, m.get('2')
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
      assert_equal 'thompson', m.cas('thom', 'thompson', value.memcache_cas)
      assert_equal 'thompson', m.get('thom')

      value = m.gets('thom')
      m.delete('thom')
      assert_nil m.cas('thom', 'hartson', value.memcache_cas)
      assert_equal nil, m.get('thom')

      m.add('thom', 'hartmann')
      value = m.gets('thom')
      m.set('thom', 'foo')
      assert_nil m.cas('thom', 'hartson', value.memcache_cas)
      assert_equal 'foo', m.get('thom')
    end
  end
end
