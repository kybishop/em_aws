# Copyright 2011 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License'). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the 'license' file accompanying this file. This file is
# distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

require 'spec_helper'
require 'eventmachine'
require 'evma_httpserver'
module AWS::Core
  module Http
    class EMFooIO
      def path
        '/my_path/test.text'
      end
    end

    # A slow server for testing timeout,
    # borrowed from: http://www.igvita.com/2008/05/27/ruby-eventmachine-the-speed-demon/
    class SlowServer < EventMachine::Connection
      include EventMachine::HttpServer

      def process_http_request
        resp = EventMachine::DelegatedHttpResponse.new( self )

        sleep 2 # Simulate a long running request

        resp.status = 200
        resp.content = 'Hello World!'
        resp.send_response
      end
    end

    describe EMHttpHandler do
      let(:handler) { EMHttpHandler.new(default_request_options) }

      let(:default_request_options) { Hash.new }

      let(:req) do
        r = Http::Request.new
        r.host = 'foo.bar.com'
        r.uri = '/my_path/?foo=bar'
        r.body_stream = StringIO.new('myStringIO')

        r
      end

      let(:resp) { Http::Response.new }

      it 'should be accessible from AWS as well as AWS::Core' do
        AWS::Http::EMHttpHandler.new
          .should be_an(AWS::Core::Http::EMHttpHandler)
      end

      it 'should not timeout' do
        EM.synchrony do
          response = Http::Response.new
          request = Http::Request.new
          request.host = '127.0.0.1'
          request.port = '8081'
          request.uri = '/'
          request.body_stream = StringIO.new('myStringIO')

          # turn on our test server
          EventMachine::run do
            EventMachine::start_server request.host, request.port, SlowServer
          end

          handler.stub(:get_url).and_return('http://127.0.0.1:8081')

          handler.handle(request,response)

          response.network_error.should be_nil

          EM.stop
        end
      end

      it 'should timeout after 0.1 seconds' do
        pending 'need to fix the listed TODO'
        EM.synchrony do
          response = Http::Response.new
          request = Http::Request.new
          request.host = '127.0.0.1'
          request.port = '8081'
          request.uri = '/'
          request.body_stream = StringIO.new('myStringIO')

          # turn on our test server
          EventMachine::run do
            EventMachine::start_server request.host, request.port, SlowServer
          end

          handler.stub(:get_url).and_return('http://127.0.0.1:8081')

          # TODO(kjb) request.read_timeout used to be passed to the client:
          # https://github.com/JoshMcKin/em_aws/blob/master/lib/aws/core/http/em_http_handler.rb#L155
          # Fix this spec so timeouts are still tested
          request.stub(:read_timeout).and_return(0.01)

          handler.handle(request, response)

          response.network_error.should be_a(Timeout::Error)

          EM.stop
        end
      end

      describe '#handle' do
        context 'timeouts' do
          it 'should rescue Timeout::Error' do
            handler
              .stub(:send_request)
              .and_raise(Timeout::Error)

            expect {
              handler.handle(req, resp)
            }.to_not raise_error
          end

          it 'should rescue Errno::ETIMEDOUT' do
            handler
              .stub(:send_request)
              .and_raise(Errno::ETIMEDOUT)

            expect {
              handler.handle(req, resp)
            }.to_not raise_error
          end

          it 'should indicate that there was a network_error' do
            handler
              .stub(:send_request)
              .and_raise(Errno::ETIMEDOUT)

            handler.handle(req, resp)

            resp.network_error?.should be_true
          end
        end

        context 'default request options' do
          before(:each) do
            handler
              .stub(:default_request_options)
              .and_return(:foo => 'BAR', :private_key_file => 'blarg')
          end

          it 'passes extra options through to synchrony' do
            handler.default_request_options[:foo].should == 'BAR'
          end

          it 'uses the default when the request option is not set' do
            #puts handler.default_request_options
            handler.default_request_options[:private_key_file].should == 'blarg'
          end
        end
      end
    end
  end
end
