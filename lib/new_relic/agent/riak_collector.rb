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

      # Create connection to riak host using persistent riak client
      def connect(settings={})
        config = settings[:settings]
        @riak_client = Riak::Client.new( :host     => config[:riak_collector_host],
                                         :pb_port  => config[:riak_collector_port],
                                         :protocol => 'pbc',)
      end

      # The path to the certificate file used to verify the SSL
      # connection if verify_peer is enabled
      def cert_file_path
        File.expand_path(File.join(control.newrelic_root, 'cert', 'cacert.pem'))
      end

      private

      # The path on the server that we should post our data to
      def remote_method_uri(method, format='ruby')
        uri = "/#{method}"
        uri
      end

      def metric_data(stats_hash)
        ::NewRelic::Agent.logger.debug "metric_data events not yet supported"
      end

      def error_data(unsent_errors)
        ::NewRelic::Agent.logger.debug "error_data events not yet supported"
      end

      def transaction_sample_data(traces)
        ::NewRelic::Agent.logger.debug "transaction_sample_data events not yet supported"
      end

      def sql_trace_data(sql_traces)
        ::NewRelic::Agent.logger.debug "sql_trace_data events not yet supported"
      end

      def profile_data(profile)
        ::NewRelic::Agent.logger.debug "profile_data events not yet supported"
      end

      def get_agent_commands
        ::NewRelic::Agent.logger.debug "error_data events not yet supported"
      end

      def agent_command_results(results)
        ::NewRelic::Agent.logger.debug "agent_command_results not yet supported"
      end

      def get_xray_metadata(xray_ids)
        ::NewRelic::Agent.logger.debug "xray_metadata events not yet supported"
      end

      # Send fine-grained analytic data to the collector.
      def analytic_event_data(data)
        #data = data.map { |hash| [hash] }
        invoke_remote(:analytic_event_data, @agent_id, data)
      end

      # send a message via post to the actual server. This attempts
      # to automatically compress the data via zlib if it is large
      # enough to be worth compressing, and handles any errors the
      # server may return
      def invoke_remote(method, *args)
        now = Time.now

        data = args
        size = nil

        ::NewRelic::Agent.logger.debug "#{method}: #{data}"

        size = data.size

        uri = "#{method}"

        @audit_logger.log_request(uri, args, @marshaller)
        response = send_request(:data      => data,
                                :uri       => uri,
                                :collector => @collector)
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
      #  - :collector => a URI object that responds to the 'name' method
      #                    and returns the name of the collector to
      #                    contact
      #  - :data => the data to send as the body of the request
      def send_request(opts)
        ::NewRelic::Agent.logger.info "sending data to #{opts[:uri]} bucket"
        bucket = @riak_client.bucket(opts[:uri])
        opts[:data].each do |container|
          next if container.nil?
          container.each do |data|
            begin
              data = @marshaller.dump(data)
            rescue JsonError
              @marshaller = PrubyMarshaller.new
              retry
            end
            ::NewRelic::Agent.logger.debug "serialized data: #{data}"
            event = bucket.new
            event.raw_data = data
            event.store()
          end
        end
      end

    end
  end
end
