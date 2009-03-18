require 'socket'
require 'thread'
require 'timeout'

class Memcache
  class Server
    CONNECT_TIMEOUT = 1.0
    RETRY_DELAY     = 10.0
    DEFAULT_PORT    = 11211

    attr_reader :host, :port, :status
    attr_accessor :retry_at
    
    class MemcacheError   < StandardError; end
    class ConnectionError < MemcacheError; end
    class ServerError     < MemcacheError; end
    class ClientError     < MemcacheError; end
    class ServerDown      < MemcacheError; end

    def initialize(opts)
      @host   = opts[:host]
      @port   = opts[:port]   || DEFAULT_PORT
      @status = 'NOT CONNECTED'

      @readonly    = opts[:readonly]
      @multithread = opts[:multithread]      
    end

    def inspect
      "<Memcache::Server: %s:%d (%s)>" % [@host, @port, @status]
    end

    def name
      "#{host}:#{port}"
    end

    def alive?
      not socket.nil?
    end

    def retry?
      @retry_at.nil? or @retry_at < Time.now
    end

    def multithread?
      @multithread
    end

    def readonly?
      @readonly
    end

    def close
      # Close the socket. The server is not considered dead.
      mutex.lock if multithread?
      @socket.close if @socket and not @socket.closed?
      @socket   = nil
      @retry_at = nil
      @status   = "NOT CONNECTED"
    ensure
      mutex.unlock if multithread?
    end

    def stats
      next unless alive?
      stats = {}
      send_command('stats') do |response|
        while response do
          break if response == "END\r\n"

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
      stats
    end

    def flush_all(delay = nil)
      send_command("flush_all #{delay}")
    end

    alias clear flush_all

    def get(key)
      send_command("get #{key}") do |response|
        return nil if response == "END\r\n"
          
        key, flags, length = match_response!(response, /^VALUE (.+) (.+) (.+)/)

        value = socket.read(length.to_i)

        match_response!(socket.read(2), "\r\n")
        match_response!(socket.gets, "END\r\n")
        return value
      end
    end

    def get_multi(keys)
      values = {}
      send_command("get #{keys.join(' ')}") do |response|
        while response do
          return values if response == "END\r\n"

          key, flags, length = match_response!(response, /^VALUE (.+) (.+) (.+)/)
          values[key] = socket.read(length.to_i)

          match_response!(socket.read(2), "\r\n")
          response = socket.gets
        end
        unexpected_eof!
      end
    end

    def incr(key, amount = 1)
      check_writable!
      if amount < 0
        method = 'decr'
        amount = amount.abs
      else
        method = 'incr'
      end

      response = send_command("#{method} #{key} #{amount}")
      return nil if response == "NOT_FOUND\r\n"
      return response.to_i
    end

    def delete(key, expiry = 0)
      check_writable!
      send_command("delete #{key} #{expiry}") == "DELETED\r\n"
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
      send_command ["#{method} #{key} 0 #{expiry} #{value.to_s.size}", value]
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
      retried = false
      begin
        mutex.lock if multithread?
        command = command.join("\r\n") if command.kind_of?(Array)
        socket.write("#{command}\r\n")
        response = socket.gets
        
        unexpected_eof! if response.nil?
        if response =~ /^(ERROR|CLIENT_ERROR|SERVER_ERROR) (.*)\r\n/
          raise ($1 == 'SERVER_ERROR' ? ServerError : ClientError), $2
        end

        block_given? ? yield(response) : response

      rescue ClientError, ServerError, SocketError, SystemCallError, IOError => error
        if not retried
          # Close the socket and retry once.
          close
          retried = true
          retry
        else
          # Mark the server dead and raise an error.
          kill(error.message)

          # Reraise the error if it is a MemcacheError
          raise error if error.kind_of?(MemcacheError)

          # Reraise as a ConnectionError
          new_error = ConnectionError.new("#{error.class}: #{error.message}")
          new_error.set_backtrace(error.backtrace)
          raise new_error
        end
      ensure
        mutex.unlock if multithread?
      end
    end

    def socket
      return @socket if @socket and not @socket.closed?
      raise ServerDown, "will retry at #{retry_at}" unless retry?

      begin
        # Attempt to connect.
        mutex.lock if multithread?
        @socket = timeout(CONNECT_TIMEOUT) do
          TCPSocket.new(host, port)
        end

        if Socket.constants.include? 'TCP_NODELAY'
          @socket.setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1
        end
        @retry_at = nil
        @status   = 'CONNECTED'
      rescue SocketError, SystemCallError, IOError, Timeout::Error => e
        # Connection failed.
        kill(e.message)
        raise ServerDown, e.message
      ensure
        mutex.unlock if multithread?
      end

      @socket
    end

    def unexpected_eof!
      raise ConnectionError, 'unexpected end of file' 
    end

    def kill(reason = 'Unknown error')
      # Mark the server as dead and close its socket.
      @socket.close if @socket and not @socket.closed?
      @socket   = nil
      @retry_at = Time.now + RETRY_DELAY  
      @status   = "DEAD: %s, will retry at %s" % [reason, @retry_at]
    end

    def mutex
      @mutex ||= Mutex.new
    end
  end
end
