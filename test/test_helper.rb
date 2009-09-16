require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'mocha'
require 'pp'

$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../lib')
require 'memcache'

class Test::Unit::TestCase
  def start_memcache(*ports)
    ports.each do |port|
      system("memcached -p #{port} -U 0 -d -P /tmp/memcached_#{port}.pid")
    end
    sleep 0.1
  end

  def stop_memcache(*ports)
    ports.each do |port|
      pid = File.read("/tmp/memcached_#{port}.pid").to_i
      Process.kill('TERM', pid)
    end
  end
end

class Module
  def const_override(const, value)
    old_value = const_get(const)
    old_verbose, $VERBOSE = $VERBOSE, nil
    begin
      const_set(const, value)
      yield
    ensure
      const_set(const, old_value)
      $VERBOSE = old_verbose
    end
  end
end
