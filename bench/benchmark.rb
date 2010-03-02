require 'rubygems'
require 'benchmark'
require 'pp'

$:.unshift(File.dirname(__FILE__) + '/../lib')
require  'memcache'

def array(*args)
  arr = []
  args.each do |arg|
    if arg.kind_of?(Enumerable)
      arr.concat(arg.to_a)
    else
      arr << arg
    end
  end
  arr
end

CHARS = array('a'..'z', 'A'..'Z', '0'..'9', '_', '+', '-')
def rand_string(len)
  str = ''
  len.times do
    str << pick_rand(CHARS)
  end
  str
end

def pick_rand(items)
  i = rand(items.size)
  items[i]
end

def pick_mod(items, i)
  i = i % items.size
  items[i]
end

class MemcacheBench
  attr_reader :num_items, :key_length, :val_length, :keys, :vals, :n

  def initialize(opts = {})
    @n          = opts[:n] || 100_000
    @num_items  = opts[:num_items] || 5000
    @key_length = array(opts[:key_length] || 10)
    @val_length = array(opts[:val_length] || 100)

    puts "N = #{@n}"
    puts "key_length: #{@key_length.join(' or ')}"
    puts "val_length: #{@val_length.join(' or ')}"
    puts "Generating #{@num_items} random keys and values..."
    @keys = []
    @vals = []
    @num_items.times do
      @keys << rand_string( pick_rand(@key_length) )
      @vals << rand_string( pick_rand(@val_length) )
    end

    Benchmark.bm(36) do |x|
      @bench = x
    end
  end

  def bench(name, nkeys = 1, &block)
    if nkeys > 1
      keyseq = keys + keys
      block  = lambda do |i|
        i = i % keys.size
        yield(keyseq[i, nkeys])
      end
      name = "#{name}-#{nkeys}"
    elsif block.arity == 1
      block = lambda {|i| yield(pick_mod(keys, i))}
    else
      block = lambda {|i| yield(pick_mod(keys, i), pick_mod(vals, i))}
    end

    @bench.report(name) do
      (n/nkeys).times do |i|
        block.call(i)
      end
    end
  end
end

def init_servers(*ports)
  servers = []
  ports.each do |port|
    system("memcached -p #{port} -U 0 -d -P /tmp/memcached_#{port}.pid")
    servers << "127.0.0.1:#{port}"
    sleep 0.3
  end
  memcache = yield(servers)
  memcache.flush_all
  memcache
end

def ___
  puts('=' * 81)
end

puts `uname -a`
puts "Ruby #{RUBY_VERSION}p#{RUBY_PATCHLEVEL}"

ns = 'namespace'
memcache      = init_servers(10000,10001) {|s| Memcache.new(:servers => s, :namespace => ns)}
native        = init_servers(10002,10003) {|s| Memcache.new(:servers => s, :namespace => ns, :native => true)}
native_nowrap = init_servers(10004,10005) {|s| Memcache::NativeServer.new(:servers => s, :prefix => "#{ns}:")}

b = MemcacheBench.new(:num_items => 5000, :n => 100_000, :key_length => 20, :val_length => 100)

2.times do
  ___
  b.bench( 'set:native-nowrap'      ) {|key, val| native_nowrap.set(key, val) }
  b.bench( 'get:native-nowrap'      ) {|key     | native_nowrap.get(key)      }
  b.bench( 'get:native-nowrap', 100 ) {|keys    | native_nowrap.get(keys)     }
  ___
  b.bench( 'set:native'      ) {|key, val| native.set(key, val, :raw => true) }
  b.bench( 'get:native'      ) {|key     | native.get(key,      :raw => true) }
  b.bench( 'get:native', 100 ) {|keys    | native.get(keys,     :raw => true) }
  ___
  b.bench( 'set:ruby'      ) {|key, val| memcache.set(key, val, :raw => true) }
  b.bench( 'get:ruby'      ) {|key     | memcache.get(key,      :raw => true) }
  b.bench( 'get:ruby', 100 ) {|keys    | memcache.get(keys,     :raw => true) }
  ___
end
