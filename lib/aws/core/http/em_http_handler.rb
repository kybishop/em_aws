# http://docs.amazonwebservices.com/AWSRubySDK/latest/
require 'hot_tub'
require 'em-synchrony'
require 'em-synchrony/em-http'
require 'em-synchrony/thread'
module AWS
  module Core
    module Http

      # An EM-Synchrony implementation for Fiber based asynchronous ruby application.
      # See https://github.com/igrigorik/async-rails and
      # http://www.mikeperham.com/2010/04/03/introducing-phat-an-asynchronous-rails-app/
      # for examples of Aync-Rails application
      #
      # In Rails add the following to your aws.rb initializer
      #
      # require 'aws-sdk'
      # require 'aws/core/http/em_http_handler'
      # AWS.config(
      #   :http_handler => AWS::Http::EMHttpHandler.new(
      #     :proxy => {:host => '127.0.0.1',    # proxy address
      #        :port => 9000,                 # proxy port
      #        :type => :socks5},
      #   :pool_size => 20,   # Default is 1, set to > 0 to enable pooling
      #   :async => false))   # If set to true all requests are handle asynchronously
      #                       # and initially return nil
      #
      # EM-AWS exposes all connections options for EM-Http-Request at initialization
      # For more information on available options see https://github.com/igrigorik/em-http-request/wiki/Issuing-Requests#available-connection--request-parameters
      # If Options from the request section of the above link are present, they
      # set on every request but may be over written by the request object
      class EMHttpHandler

        EM_PASS_THROUGH_ERRORS = [
          NoMethodError, FloatDomainError, TypeError, NotImplementedError,
          SystemExit, Interrupt, SyntaxError, RangeError, NoMemoryError,
          ArgumentError, ZeroDivisionError, LoadError, NameError,
          LocalJumpError, SignalException, ScriptError,
          SystemStackError, RegexpError, IndexError,
        ]

        # Constructs a new HTTP handler using EM-Synchrony.
        # @param [Hash] options Default options to send to EM-Synchrony on
        # each request. These options will be sent to +get+, +post+,
        # +head+, +put+, or +delete+ when a request is made. Note
        # that +:body+, +:head+, +:parser+, and +:ssl_ca_file+ are
        # ignored. If you need to set the CA file see:
        # https://github.com/igrigorik/em-http-request/wiki/Issuing-Requests#available-connection--request-parameters
        def initialize(options = {})
          @client_options = parse_client_options options
          @pool = HotTub::Session.new(parse_pool_options options) do |url|
            EM::HttpRequest.new(url, @client_options)
          end
        end

        def handle(request, response, &read_block)
          if EM::reactor_running?
            process_request(request, response, &read_block)
          else
            EM.synchrony do
              process_request(request, response, &read_block)

              @pool.close_all
              EM.stop
            end
          end
        end

      private

        def parse_client_options(options)
          client_options = options.dup

          client_options.delete(:pool_size)
          client_options.delete(:never_block)
          client_options.delete(:blocking_timeout)

          client_options[:inactivity_timeout] ||= 0
          client_options[:connect_timeout] ||= 10
          client_options[:keepalive] = true

          client_options
        end

        def parse_pool_options(options)
          {
            :with_pool => true,
            :size => options[:pool_size] ? options[:pool_size].to_i : 5,
            :never_block => options[:never_block] ? true : false,
            :blocking_timeout => options[:blocking_timeout] || 10
          }
        end

        # Builds and attempts the request. Occasionally under load
        # em-http-request returns a status of 0 for various http timeouts, see:
        # https://github.com/igrigorik/em-http-request/issues/76
        # https://github.com/eventmachine/eventmachine/issues/175
        def process_request(request, response, &read_block)
          request_options = parse_request_options(request)

          begin
            http_response = send_request(request, request_options, &read_block)

            unless request_options[:async]
              response.status = http_response.response_header.status.to_i
              raise Timeout::Error if response.status == 0
              response.headers = send_request_headers(http_response)
              response.body = http_response.response
            end
          rescue Timeout::Error => error
            response.network_error = error
          rescue *EM_PASS_THROUGH_ERRORS => error
            raise error
          rescue Exception => error
            response.network_error = error
          end

          nil
        end

        def parse_request_options(request)
          request_options = @client_options.merge(parse_request_headers request)
          request_options[:query] = request.querystring

          if request.body_stream.respond_to?(:path)
            request_options[:file] = request.body_stream.path
          else
            request_options[:body] = request.body.to_s
          end

          request_options[:path] = request.path if request.path

          request_options
        end

        def get_url(request)
          if request.use_ssl?
            "https://#{request.host}:#{request.port}"
          else
            "http://#{request.host}:#{request.port}"
          end
        end

        def parse_request_headers(request)
          # Net::HTTP adds a content-type (1.8.7+) and accept-encoding (2.0.0+)
          # to the request if these headers are not set.  Setting a default
          # empty value defeats this.
          #
          # Removing these are necessary for most services to no break request
          # signatures as well as dynamodb crc32 checks (these fail if the
          # response is gzipped).
          headers = { 'content-type' => '', 'accept-encoding' => '' }

          request.headers.each_pair do |key,value|
            headers[key] = value.to_s
          end

          { :head => headers }
        end

        def send_request(request, options = {}, &read_block)
          # aget, apost, aput, adelete, ahead
          method = "a#{request.http_method}".downcase.to_sym
          url = get_url(request)

          @pool.run(url) do |connection|
            req = connection.send(method, options)
            req.stream(&read_block) if block_given?

            EM::Synchrony.sync(req) unless options[:async]
          end
        end

        # AWS needs all header keys downcased and values need to be arrays
        def send_request_headers(response)
          response_headers = response.response_header.raw.to_hash
          aws_headers = {}

          response_headers.each_pair do |k, v|
            key = k.downcase
            #['x-amz-crc32', 'x-amz-expiration',
            # 'x-amz-restore', 'x-amzn-errortype']
            if v.is_a?(Array)
              aws_headers[key] = v
            else
              aws_headers[key] = [v]
            end
          end

          response_headers.merge(aws_headers)
        end
      end
    end
  end

  # We move this from AWS::Http to AWS::Core::Http, but we want the
  # previous default handler to remain accessible from its old namespace
  # @private
  module Http
    class EMHttpHandler < Core::Http::EMHttpHandler; end
  end
end
