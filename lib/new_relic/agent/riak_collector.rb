# encoding: utf-8
# This file is distributed under New Relic's license terms.
# See https://github.com/newrelic/rpm/blob/master/LICENSE for complete details.

require 'zlib'
require 'new_relic/agent/audit_logger'
require 'riak'

module NewRelic
  module Agent
    class RiakCollector < NewRelicService
      # Store metrics in a riak backing store

      PROTOCOL_VERSION = 12
      # 1f147a42: v10 (tag 3.5.3.17)
      # cf0d1ff1: v9 (tag 3.5.0)
      # 14105: v8 (tag 2.10.3)
      # (no v7)
      # 10379: v6 (not tagged)
      # 4078:  v5 (tag 2.5.4)
      # 2292:  v4 (tag 2.3.6)
      # 1754:  v3 (tag 2.3.0)
      # 534:   v2 (shows up in 2.1.0, our first tag)

      attr_accessor :request_timeout, :riak_client
      attr_reader :collector, :marshaller, :metric_id_cache, :last_metric_harvest_time

      def initialize(license_key=nil, collector=control.server)
        @license_key = license_key || Agent.config[:license_key]
        @collector = collector
        @request_timeout = Agent.config[:timeout]
        @metric_id_cache = {}
        @last_metric_harvest_time = Time.now

        @audit_logger = ::NewRelic::Agent::AuditLogger.new
        Agent.config.register_callback(:'audit_log.enabled') do |enabled|
          @audit_logger.enabled = enabled
        end
        Agent.config.register_callback(:ssl) do |ssl|
          if !ssl
            ::NewRelic::Agent.logger.warn("Agent is configured not to use SSL when communicating with New Relic's servers")
          else
            ::NewRelic::Agent.logger.debug("Agent is configured to use SSL")
          end
        end

        Agent.config.register_callback(:marshaller) do |marshaller|
          begin
            if marshaller == 'json'
              require 'json'
              @marshaller = JsonMarshaller.new
            else
              @marshaller = PrubyMarshaller.new
            end
          rescue LoadError
            @marshaller = PrubyMarshaller.new
          end
        end
      end

      def connect(settings={})
        @riak_client = Riak::Client.new( :protocol => settings['protocol'],
                          :host     => settings['host'],
                          :port     => settings['port'],)
      end

      # The path to the certificate file used to verify the SSL
      # connection if verify_peer is enabled
      def cert_file_path
        File.expand_path(File.join(control.newrelic_root, 'cert', 'cacert.pem'))
      end

      private

      # The path on the server that we should post our data to
      def remote_method_uri(method, format='ruby')
        params = {'run_id' => @agent_id, 'marshal_format' => format}
        uri = "/agent_listener/#{PROTOCOL_VERSION}/#{@license_key}/#{method}"
        uri << '?' + params.map do |k,v|
          next unless v
          "#{k}=#{v}"
        end.compact.join('&')
        uri
      end

      # send a message via post to the actual server. This attempts
      # to automatically compress the data via zlib if it is large
      # enough to be worth compressing, and handles any errors the
      # server may return
      def invoke_remote(method, *args)
        now = Time.now

        data, size = nil
        begin
          data = @marshaller.dump(args)
        rescue JsonError
          @marshaller = PrubyMarshaller.new
          retry
        end

        data, encoding = compress_request_if_needed(data)
        size = data.size

        uri = remote_method_uri(method, @marshaller.format)
        full_uri = "#{@collector}#{uri}"

        @audit_logger.log_request(full_uri, args, @marshaller)
        response = send_request(:data      => data,
                                :uri       => uri,
                                :encoding  => encoding,
                                :collector => @collector)
        @marshaller.load(decompress_response(response))
      rescue NewRelic::Agent::ForceRestartException => e
        ::NewRelic::Agent.logger.debug e.message
        raise
      ensure
        record_supportability_metrics(method, now, size)
      end

      # Posts to the specified server
      #
      # Options:
      #  - :uri => the path to request on the server (a misnomer of
      #              course)
      #  - :encoding => the encoding to pass to the server
      #  - :collector => a URI object that responds to the 'name' method
      #                    and returns the name of the collector to
      #                    contact
      #  - :data => the data to send as the body of the request
      def send_request(opts)
        request = Net::HTTP::Post.new(opts[:uri], 'CONTENT-ENCODING' => opts[:encoding], 'HOST' => opts[:collector].name)
        request['user-agent'] = user_agent
        request.content_type = "application/octet-stream"
        request.body = opts[:data]

        response = nil
        http = http_connection
        http.read_timeout = nil
        NewRelic::TimerLib.timeout(@request_timeout) do
          ::NewRelic::Agent.logger.debug "Sending request to #{opts[:collector]}#{opts[:uri]}"
          response = http.request(request)
        end
        case response
        when Net::HTTPSuccess
          true # fall through
        when Net::HTTPUnauthorized
          raise LicenseException, 'Invalid license key, please contact support@newrelic.com'
        when Net::HTTPServiceUnavailable
          raise ServerConnectionException, "Service unavailable (#{response.code}): #{response.message}"
        when Net::HTTPGatewayTimeOut
          raise Timeout::Error, response.message
        when Net::HTTPRequestEntityTooLarge
          raise UnrecoverableServerException, '413 Request Entity Too Large'
        when Net::HTTPUnsupportedMediaType
          raise UnrecoverableServerException, '415 Unsupported Media Type'
        else
          raise ServerConnectionException, "Unexpected response from server (#{response.code}): #{response.message}"
        end
        response
      rescue Timeout::Error, EOFError, SystemCallError, SocketError => e
        # These include Errno connection errors, and all signify that the
        # connection may be in a bad state, so drop it and re-create if needed.
        close_shared_connection
        raise NewRelic::Agent::ServerConnectionException, "Recoverable error connecting to #{@collector}: #{e}"
      end

    end
  end
end
