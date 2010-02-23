require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'mocha'
require 'pp'

$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../lib')
require 'memcache'

system 'killall memcached 2> /dev/null'

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
