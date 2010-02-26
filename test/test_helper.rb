require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'mocha'
require 'pp'

$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../lib')
require 'memcache'

class Test::Unit::TestCase
  @@servers ||= {}
  def init_memcache(*ports)
    ports.each do |port|
      @@servers[port] ||= start_memcache(port)
    end

    @memcache = yield
    @memcache.flush_all
  end

  def m
    @memcache
  end

  def start_memcache(port)
    system("memcached -p #{port} -U 0 -d -P /tmp/memcached_#{port}.pid")
    sleep 1
    File.read("/tmp/memcached_#{port}.pid")
  end

  def self.with_prefixes(*prefixes)
    # Define new test_* methods that calls super for every prefix. This only works for
    # methods that are mixed in, and should be called before you define custom test methods.
    opts = prefixes.last.is_a?(Hash) ? prefixes.last : {}
    instance_methods.each do |method_name|
      next unless method_name =~ /^test_/
      next if opts[:except] and opts[:except].include?(method_name)
      next if opts[:only] and not opts[:only].include?(method_name)

      define_method(method_name) do |*args|
        prefixes.each do |prefix|
          assert_equal prefix, m.prefix = prefix
          assert_equal prefix, m.prefix
          super
          assert_equal nil, m.prefix = nil
          assert_equal nil, m.prefix
        end
      end
    end
  end
end
