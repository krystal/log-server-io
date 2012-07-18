require 'socket'

module Atech
  class NetworkLogIO < IO
    attr_reader :options

    def initialize(options)
      @options = options

      raise(ArgumentError, ':host is required') unless @options[:host]
      raise(ArgumentError, ':app_name is required') unless @options[:app_name]
      raise(ArgumentError, ':log_name is required') unless @options[:log_name]

      @options[:port]         ||= 4455
      @options[:buffer_size]  ||= 512

      @socket = UDPSocket.new
      empty_buffer!
    end

    def empty_buffer!
      @buffer = fresh_buffer
    end

    def fresh_buffer
      [@options[:app_name].bytesize, @options[:app_name], @options[:log_name].bytesize, @options[:log_name]].pack('nA*nA*')
    end

    def write(data)
      data.force_encoding('BINARY')
      data = data[0,@options[:buffer_size] - fresh_buffer.bytesize - 2]

      ## Pre-flush if the existing buffer plus the new data plus the terminator would be more than the buffer size
      if @buffer.bytesize + 2 + data.bytesize + 2 > @options[:buffer_size]
        self.flush
      end

      @buffer << [data.bytesize].pack('n')
      @buffer << data

      ## Flush is autoflush is set or the buffer is full
      self.flush if @options[:auto_flush] or @buffer.bytesize + 2 >= @options[:buffer_size]

      return data.bytesize
    end

    def flush
      return self if @buffer == @api_key
      @buffer << "\0\0"
      @socket.send(@buffer, 0, @options[:host], @options[:port])
      empty_buffer!
      return self
    end

  end
end
