require 'socket'
require 'thread'
require 'timeout'

class Memcache
  class Server < Base
    CONNECT_TIMEOUT  = 1.0
    READ_RETRY_DELAY = 5.0
    DEFAULT_PORT     = 11211

    attr_reader :host, :port, :status, :retry_at

    def initialize(opts)
      @host         = opts[:host]
      @port         = opts[:port] || DEFAULT_PORT
      @strict_reads = opts[:strict_reads]
      @status       = 'NOT CONNECTED'
    end

    def clone
      self.class.new(:host => host, :port => port, :strict_reads => strict_reads?)
    end

    def inspect
      "<#{self.class.name}: %s:%d (%s)>" % [@host, @port, @status]
    end

    def name
      "#{host}:#{port}"
    end

    def alive?
      @retry_at.nil? or @retry_at < Time.now
    end

    def strict_reads?
      @strict_reads
    end

    def close(error = nil)
      # Close the socket. If there is an error, mark the server dead.
      @socket.close if @socket and not @socket.closed?
      @socket = nil

      if error
        @retry_at = Time.now + READ_RETRY_DELAY
        @status   = "DEAD: %s: %s, will retry at %s" % [error.class, error.message, @retry_at]
      else
        @retry_at = nil
        @status   = "NOT CONNECTED"
      end
    end

    def stats
      stats = {}
      read_command('stats') do |response|
        key, value = match_response!(response, /^STAT ([\w]+) (-?[\w\.\:]+)/)

        if ['rusage_user', 'rusage_system'].include?(key)
          seconds, microseconds = value.split(/:/, 2)
          microseconds ||= 0
          stats[key] = Float(seconds) + (Float(microseconds) / 1_000_000)
        else
          stats[key] = (value =~ /^-?\d+$/ ? value.to_i : value)
        end
      end
      stats
    end

    def count
      stats['curr_items']
    end

    def flush_all(delay = nil)
      write_command("flush_all #{delay}")
    end

    def get(keys, cas = nil)
      return get([keys], cas)[keys.to_s] unless keys.kind_of?(Array)
      return {} if keys.empty?

      method = cas ? 'gets' : 'get'

      results = {}
      keys = keys.collect {|key| cache_key(key)}

      read_command("#{method} #{keys.join(' ')}") do |response|
        if cas
          key, flags, length, cas = match_response!(response, /^VALUE ([^\s]+) ([^\s]+) ([^\s]+) ([^\s]+)/)
        else
          key, flags, length = match_response!(response, /^VALUE ([^\s]+) ([^\s]+) ([^\s]+)/)
        end

        value = socket.read(length.to_i)
        match_response!(socket.read(2), "\r\n")

        value.memcache_flags = flags.to_i
        value.memcache_cas   = cas

        key = input_key(key)
        results[key] = value
      end
      results
    end

    def incr(key, amount = 1)
      raise Error, "incr requires unsigned value" if amount < 0
      response = write_command("incr #{cache_key(key)} #{amount}")
      response == "NOT_FOUND\r\n" ? nil : response.slice(0..-3).to_i
    end

    def decr(key, amount = 1)
      raise Error, "decr requires unsigned value" if amount < 0
      response = write_command("decr #{cache_key(key)} #{amount}")
      response == "NOT_FOUND\r\n" ? nil : response.slice(0..-3).to_i
    end

    def delete(key)
      write_command("delete #{cache_key(key)}") == "DELETED\r\n" ? true : nil
    end

    def set(key, value, expiry = 0, flags = 0)
      return delete(key) if value.nil?
      write_command("set #{cache_key(key)} #{flags.to_i} #{expiry.to_i} #{value.to_s.size}", value)
      value
    end

    def cas(key, value, cas, expiry = 0, flags = 0)
      response = write_command("cas #{cache_key(key)} #{flags.to_i} #{expiry.to_i} #{value.to_s.size} #{cas.to_i}", value)
      response == "STORED\r\n" ? value : nil
    end

    def add(key, value, expiry = 0, flags = 0)
      response = write_command("add #{cache_key(key)} #{flags.to_i} #{expiry.to_i} #{value.to_s.size}", value)
      response == "STORED\r\n" ? value : nil
    end

    def replace(key, value, expiry = 0, flags = 0)
      response = write_command("replace #{cache_key(key)} #{flags.to_i} #{expiry.to_i} #{value.to_s.size}", value)
      response == "STORED\r\n" ? value : nil
    end

    def append(key, value)
      response = write_command("append #{cache_key(key)} 0 0 #{value.to_s.size}", value)
      response == "STORED\r\n"
    end

    def prepend(key, value)
      response = write_command("prepend #{cache_key(key)} 0 0 #{value.to_s.size}", value)
      response == "STORED\r\n"
    end

  protected

    ESCAPE = {
      " "   => '\s',
      "\t"  => '\t',
      "\n"  => '\n',
      "\v"  => '\v',
      "\f"  => '\f',
      "\\"  => '\\\\',
    }
    UNESCAPE = ESCAPE.invert

    def input_key(key)
      key = key[prefix.size..-1] if prefix # Remove prefix from key.
      key = key.gsub(/\\./) {|c| UNESCAPE[c]}
      key
    end

    def cache_key(key)
      key = key.gsub(/[\s\\]/) {|c| ESCAPE[c]}
      super(key)
    end

  private

    def match_response!(response, regexp)
      # Make sure that the response matches the protocol.
      unexpected_eof! if response.nil?
      match = response.match(regexp)
      raise ServerError, "unexpected response: #{response.inspect}" unless match

      match.to_a[1, match.size]
    end

    def send_command(*command)
      command = command.join("\r\n")
      socket.write("#{command}\r\n")
      response = socket.gets

      unexpected_eof! if response.nil?
      if response =~ /^(ERROR|CLIENT_ERROR|SERVER_ERROR) (.*)\r\n/
        raise ($1 == 'SERVER_ERROR' ? ServerError : ClientError), $2
      end

      block_given? ? yield(response) : response
    rescue Exception => e
      close(e) # Mark dead.
      raise e if e.kind_of?(Error)
      raise ConnectionError.new(e)
    end

    def write_command(*command, &block)
      retried = false
      begin
        send_command(*command, &block)
      rescue Exception => e
        puts "Memcache write error: #{e.class} #{e.to_s}"
        unless retried
          retried = true
          retry
        end
        raise(e)
      end
    end

    def read_command(command, &block)
      raise ConnectionError, "Server #{name} dead, will retry at #{retry_at}" unless alive?
      send_command(command) do |response|
        while response do
          return if response == "END\r\n"
          yield(response)
          response = socket.gets
        end
        unexpected_eof!
      end
    rescue Exception => e
      puts "Memcache read error: #{e.class} #{e.to_s}"
      raise(e) if strict_reads?
    end

    def socket
      if @socket.nil? or @socket.closed?
        # Attempt to connect.
        @socket = timeout(CONNECT_TIMEOUT) do
          TCPSocket.new(host, port)
        end
        if Socket.constants.include? 'TCP_NODELAY'
          @socket.setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1
        end

        @retry_at = nil
        @status   = 'CONNECTED'
      end
      @socket
    end

    def unexpected_eof!
      raise Error, 'unexpected end of file'
    end
  end
end
