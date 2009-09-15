require 'socket'
require 'thread'
require 'timeout'

class Memcache
  class Server
    CONNECT_TIMEOUT = 1.0
    RETRY_DELAY     = 5.0 # Only used for reads.
    DEFAULT_PORT    = 11211

    attr_reader :host, :port, :status, :retry_at
    attr_writer :strict_reads

    class MemcacheError < StandardError; end
    class ServerError   < MemcacheError; end
    class ClientError   < MemcacheError; end

    def initialize(opts)
      @host         = opts[:host]
      @port         = opts[:port] || DEFAULT_PORT
      @readonly     = opts[:readonly]
      @strict_reads = opts[:strict_reads]
      @status       = 'NOT CONNECTED'
    end

    def inspect
      "<Memcache::Server: %s:%d (%s)>" % [@host, @port, @status]
    end

    def name
      "#{host}:#{port}"
    end

    def alive?
      @retry_at.nil? or @retry_at < Time.now
    end

    def readonly?
      @readonly
    end

    def strict_reads?
      @strict_reads
    end

    def close(error = nil)
      # Close the socket. If there is an error, mark the server dead.
      @socket.close if @socket and not @socket.closed?
      @socket = nil
      
      if error
        @retry_at = Time.now + RETRY_DELAY
        @status   = "DEAD: %s: %s, will retry at %s" % [error.class, error.message, @retry_at]
      else
        @retry_at = nil
        @status   = "NOT CONNECTED"
      end
    end

    def stats
      stats = {}
      read_command('stats') do |response|
        while response do
          return stats if response == "END\r\n"

          key, value = match_response!(response, /^STAT ([\w]+) ([\w\.\:]+)/)

          if ['rusage_user', 'rusage_system'].include?(key)
            seconds, microseconds = value.split(/:/, 2)
            microseconds ||= 0
            stats[key] = Float(seconds) + (Float(microseconds) / 1_000_000)
          else
            stats[key] = (value =~ /^\d+$/ ? value.to_i : value)
          end

          response = socket.gets
        end
      end
      return {}
    end

    def flush_all(delay = nil)
      check_writable!
      write_command("flush_all #{delay}")
    end

    alias clear flush_all

    def gets(keys)
      return gets([keys])[keys.to_s] unless keys.kind_of?(Array)

      result = {}
      read_command("gets #{keys.join(' ')}") do |response|
        while response do
          return results if response == "END\r\n"

          key, flags, length, cas = match_response!(response, /^VALUE (.+) (.+) (.+) (.+)/)
          results[key] = [socket.read(length.to_i), cas]

          match_response!(socket.read(2), "\r\n")
          response = socket.gets
        end
        unexpected_eof!
      end
      return {}
    end

    def get(keys)
      return get([keys])[keys.to_s] unless keys.kind_of?(Array)

      results = {}
      read_command("get #{keys.join(' ')}") do |response|
        while response do
          return results if response == "END\r\n"

          key, flags, length = match_response!(response, /^VALUE (.+) (.+) (.+)/)
          results[key] = socket.read(length.to_i)

          match_response!(socket.read(2), "\r\n")
          response = socket.gets
        end
        unexpected_eof!
      end
      return {}
    end

    def incr(key, amount = 1)
      check_writable!
      if amount < 0
        method = 'decr'
        amount = amount.abs
      else
        method = 'incr'
      end

      response = write_command("#{method} #{key} #{amount}")
      return nil if response == "NOT_FOUND\r\n"
      return response.to_i
    end

    def delete(key, expiry = 0)
      check_writable!
      write_command("delete #{key} #{expiry}") == "DELETED\r\n"
    end

    def set(key, value, expiry = 0)
      if value
        store(:set, key, value, expiry)
        value
      else
        delete(key)
      end
    end

    def add(key, value, expiry = 0)
      response = store(:add, key, value, expiry)
      response == "STORED\r\n" ? value : nil
    end

  private

    def store(method, key, value, expiry)
      check_writable!
      write_command ["#{method} #{key} 0 #{expiry} #{value.to_s.size}", value]
    end

    def check_writable!
      raise MemcacheError, "Update of readonly cache" if readonly?
    end

    def match_response!(response, regexp)
      # Make sure that the response matches the protocol.
      unexpected_eof! if response.nil?
      match = response.match(regexp)
      raise ServerError, "unexpected response: #{response.inspect}" unless match

      match.to_a[1, match.size]
    end

    def send_command(command)
      command = command.join("\r\n") if command.kind_of?(Array)
      socket.write("#{command}\r\n")
      response = socket.gets
      
      unexpected_eof! if response.nil?
      if response =~ /^(ERROR|CLIENT_ERROR|SERVER_ERROR) (.*)\r\n/
        raise ($1 == 'SERVER_ERROR' ? ServerError : ClientError), $2
      end
      
      block_given? ? yield(response) : response
    end

    def write_command(command, &block)
      retried = false
      begin
        send_command(command, &block)
      rescue Exception => e
        puts "Memcache write error: #{e.class}: #{e.to_s}"
        unless retried
          # Close the socket and retry once.
          retried = true
          close
          retry
        end
        close(e) # Mark dead.
        raise(e)
      end
    end

    def read_command(command, &block)
      raise MemcacheError, "Server dead, will retry at #{retry_at}" unless alive?
      send_command(command, &block)
    rescue Exception => e
      puts "Memcache read error: #{e.class}: #{e.to_s}"
      close(e) # Mark dead.
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
      raise MemcacheError, 'unexpected end of file' 
    end
  end
end
