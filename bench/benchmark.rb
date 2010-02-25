require 'rubygems'
require 'benchmark'
require 'ruby-debug' if ENV['DEBUG']
require 'pp'

puts `uname -a`
puts "Ruby #{RUBY_VERSION}p#{RUBY_PATCHLEVEL}"

$:.unshift(File.dirname(__FILE__) + '/../lib')
require  'memcache'

class Bench
  class InvalidTest < StandardError; end

  MEMCACHED_PORTS = [10001, 10002]

  def initialize(loops = nil, stack_depth = nil)
    @loops = (loops || 20000).to_i
    @stack_depth = (stack_depth || 0).to_i

    puts "Loops is #{@loops}"
    puts "Stack depth is #{@stack_depth}"

    @m_value = Marshal.dump( @small_value = ["testing"] )
    @m_large_value = Marshal.dump(
      @large_value = [{"test" => "1", "test2" => "2", Object.new => "3", 4 => 4, "test5" => 2**65}] * 2048
    )

    puts "Small value size is: #{@m_value.size} bytes"
    puts "Large value size is: #{@m_large_value.size} bytes"

    @keys = [
      @k1 = "Short",
      @k2 = "Sym1-2-3::45" * 8,
      @k3 = "Long" * 40,
      @k4 = "Medium" * 8,
      @k5 = "Medium2" * 8,
      @k6 = "Long3" * 40
    ]

    stop_servers
    start_servers

    Benchmark.bm(36) do |x|
      @benchmark = x
    end
  end

  def run(level = @stack_depth)
    level > 0 ? run(level - 1) : run_without_recursion
    stop_servers
  end

private

  def start_servers
    MEMCACHED_PORTS.each do |port|
      system("memcached -p #{port} -U 0 -d -P /tmp/memcached_#{port}.pid")
      sleep 0.3
    end
  end

  def stop_servers
    MEMCACHED_PORTS.each do |port|
      file = "/tmp/memcached_#{port}.pid"
      next unless File.file?(file)

      pid = File.read(file).to_i
      Process.kill('TERM', pid) rescue nil
    end
  end

  def clients
    return @clients if @clients

    servers = MEMCACHED_PORTS.collect{|port| "127.0.0.1:#{port}"}
    @clients = {
      'Memcache'               => Memcache.new(:servers => servers, :namespace => 'namespace'),
      'Memcache:native'        => Memcache.new(:servers => servers, :namespace => 'namespace', :native => true),
      'Memcache:native:nowrap' => Memcache::NativeServer.new(:servers => servers),
    }
  end

  def benchmark_clients(test_name)
    clients.keys.sort.each do |client_name|
      client = clients[client_name]
      begin
        yield client
        @benchmark.report("#{test_name}:#{client_name}") { @loops.times { yield client } }
      rescue InvalidTest
      rescue => e
        puts "#{test_name}:#{client_name} => #{e.class}: #{e}"
        @clients = nil
      end
    end
  end

  def benchmark_hashes(hashes, test_name)
    hashes.each do |hash_name, int|
      @m = Memcached::Rails.new(:hash => hash_name)
      @benchmark.report("#{test_name}:#{hash_name}") do
        @loops.times { yield int }
      end
    end
  end

  def run_without_recursion
    benchmark_clients("set") do |c|
      if c.class == Memcache
        c.set @k1, @m_value, :expiry => 0, :raw => true
        c.set @k2, @m_value, :expiry => 0, :raw => true
        c.set @k3, @m_value, :expiry => 0, :raw => true
      elsif c.class == Memcache::NativeServer
        c.set @k1, @m_value
        c.set @k2, @m_value
        c.set @k3, @m_value
      end
    end

    benchmark_clients("get") do |c|
      if c.class == Memcache
        c.get @k1, :raw => true
        c.get @k2, :raw => true
        c.get @k3, :raw => true
      elsif c.class == Memcache::NativeServer
        c.get @k1
        c.get @k2
        c.get @k3
      end
    end

    benchmark_clients("get-multi") do |c|
      if c.class == Memcache
        c.get @keys, :raw => true
      elsif c.class == Memcache::NativeServer
        c.get @keys
      end
    end

    benchmark_clients("append") do |c|
      c.append @k1, @m_value
      c.append @k2, @m_value
      c.append @k3, @m_value
    end

    benchmark_clients("delete") do |c|
      c.delete @k1
      c.delete @k2
      c.delete @k3
    end

    benchmark_clients("get-missing") do |c|
      c.get @k1
      c.get @k2
      c.get @k3
    end

    benchmark_clients("append-missing") do |c|
      c.append @k1, @m_value
      c.append @k2, @m_value
      c.append @k3, @m_value
    end

    benchmark_clients("set-large") do |c|
      if c.class == Memcache
        c.set @k1, @m_large_value, :expiry => 0, :raw => true
        c.set @k2, @m_large_value, :expiry => 0, :raw => true
        c.set @k3, @m_large_value, :expiry => 0, :raw => true
      elsif c.class == Memcache::NativeServer
        c.set @k1, @m_large_value, 0
        c.set @k2, @m_large_value, 0
        c.set @k3, @m_large_value, 0
      end
    end

    benchmark_clients("get-large") do |c|
      if c.class == Memcache
        c.get @k1, :raw => true
        c.get @k2, :raw => true
        c.get @k3, :raw => true
      elsif c.class == Memcache::NativeServer
        c.get @k1
        c.get @k2
        c.get @k3
      end
    end

    benchmark_clients("set-ruby") do |c|
      raise InvalidTest if c.class == Memcache::NativeServer
      c.set @k1, @value
      c.set @k2, @value
      c.set @k3, @value
    end

    benchmark_clients("get-ruby") do |c|
      raise InvalidTest if c.class == Memcache::NativeServer
      c.get @k1
      c.get @k2
      c.get @k3
    end

    benchmark_clients("set-ruby-large") do |c|
      raise InvalidTest if c.class == Memcache::NativeServer
      c.set @k1, @large_value
      c.set @k2, @large_value
      c.set @k3, @large_value
    end

    benchmark_clients("get-ruby-large") do |c|
      raise InvalidTest if c.class == Memcache::NativeServer
      c.get @k1
      c.get @k2
      c.get @k3
    end
  end
end

Bench.new(ENV["LOOPS"], ENV["STACK_DEPTH"]).run

Process.memory.each do |key, value|
  puts "#{key}: #{value/1024.0}M"
end if Process.respond_to? :memory
