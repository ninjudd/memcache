require 'socket'
require 'thread'
require 'timeout'

class Memcache
  class Server
    CONNECT_TIMEOUT = 1.0
    RETRY_DELAY     = 30.0
    DEFAULT_PORT    = 11211
    DEFAULT_WEIGHT  = 1

    attr_reader :host, :port, :status
    attr_accessor :retry, :weight

    class MemcacheError   < StandardError; end
    class ConnectionError < MemcacheError; end
    class ProtocolError   < MemcacheError; end
    class ServerError     < MemcacheError; end

    def initialize(opts)
      @host   = opts[:host]
      @port   = opts[:port]   || DEFAULT_PORT
      @weight = opts[:weight] || DEFAULT_WEIGHT
      @status = 'NOT CONNECTED'
      @multithread = opts[:multithread]
    end

    def inspect
      "<Memcache::Server: %s:%d [%d] (%s)>" % [@host, @port, @weight, @status]
    end

    def alive?
      not socket.nil?
    end

    def retry?
      @retry.nil? or @retry > Time.now
    end

    def multithread?
      @multithread
    end

    def close
      # Close the socket. The server is not considered dead.
      mutex.lock if multithread?
      @socket.close if @socket and not @socket.closed?
      @socket = nil
      @retry  = nil
      @status = "NOT CONNECTED"
    ensure
      mutex.unlock if multithread?
    end

    def decr(key, amount)
      send_command("decr #{key} #{amount}") do     
        response = socket.gets
        return nil if response == "NOT_FOUND\r\n"
        return response.to_i
      end
    end

    def cache_incr(key, amount)
      send_command("incr #{key} #{amount}") do
        response = socket.gets
        return nil if response == "NOT_FOUND\r\n"
        return response.to_i
      end
    end

    def get(key)
      send_command("get #{key}") do
        keyline = socket.gets
        raise ProtocolError, 'unexpected end of file' if keyline.nil?

        return nil if keyline == "END\r\n"
          
        key, flags, length = match!(keyline, /^VALUE (.+) (.+) (.+)/)

        value = socket.read(length.to_i)

        match!(socket.read(2), "\r\n")
        match!(socket.gets, "END\r\n")
        return value
      end
    end

    def get_multi(keys)
      values = {}
      send_command("get #{keys.join(' ')}") do

        while keyline = socket.gets do
          return values if keyline == "END\r\n"

          key, flags, length = match!(keyline, /^VALUE (.+) (.+) (.+)/)
          values[key] = socket.read(length.to_i)

          match!(socket.read(2), "\r\n")
        end

        raise ProtocolError, 'unexpected end of file'
      end
    end

    [:set, :add].each do |method|
      define_method(method) do |key, value, expiry|
        store(method, key, value, expiry)
      end
    end

  private

    def store(method, key, value, expiry)
      send_command ["#{method} #{key} 0 #{expiry} #{value.to_s.size}", value] do
        response = socket.gets

        raise ProtocolError, 'unexpected end of file' if response.nil?
        raise ServerError, "%s:\n%s" % [$1.strip, value] if response =~ /^SERVER_ERROR (.*)/
          
        response
      end
    end

    def match!(response, regexp)
      # Make sure that the response matches the protocol.
      match = response.match(regexp)
      raise ProtocolError, "unexpected response #{response.inspect}" unless match
      match.to_a[1, match.size]
    end

    def send_command(command)
      retried = false
      begin
        mutex.lock if multithread?
        command = command.join("\r\n") if command.kind_of?(Array)
        socket.write("#{command}\r\n")
        yield
      rescue ProtocolError, ServerError, SocketError, SystemCallError, IOError => error
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
      raise NotConnected unless retry?

      begin
        # Attempt to connect.
        mutex.lock if multithread?
        @socket = timeout(CONNECT_TIMEOUT) do
          TCPSocket.new(host, port)
        end

        if Socket.constants.include? 'TCP_NODELAY'
          @socket.setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1
        end
        @retry  = nil
        @status = 'CONNECTED'
      rescue SocketError, SystemCallError, IOError, Timeout::Error => e
        # Connection failed.
        kill(e.message)
        raise ConnectionError, message
      ensure
        mutex.unlock if multithread?
      end

      @socket
    end

    def kill(reason = 'Unknown error')
      # Mark the server as dead and close its socket.
      @socket.close if @socket and not @socket.closed?
      @socket = nil
      @retry  = Time.now + RETRY_DELAY  
      @status = "DEAD: %s, will retry at %s" % [reason, @retry]
    end

    def mutex
      @mutex ||= Mutex.new
    end
  end
end
