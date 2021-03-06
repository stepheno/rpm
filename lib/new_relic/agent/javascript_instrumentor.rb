# encoding: utf-8
# This file is distributed under New Relic's license terms.
# See https://github.com/newrelic/rpm/blob/master/LICENSE for complete details.

require 'base64'
require 'new_relic/agent/obfuscator'
require 'new_relic/agent/transaction_timings'

module NewRelic
  module Agent
    class JavascriptInstrumentor
      include NewRelic::Coerce

      RUM_KEY_LENGTH = 13

      def initialize(event_listener)
        event_listener.subscribe(:finished_configuring, &method(:log_configuration))
      end

      def log_configuration
        NewRelic::Agent.logger.debug("JS agent loader requested: #{NewRelic::Agent.config[:'browser_monitoring.loader']}",
                                     "JS agent loader debug: #{NewRelic::Agent.config[:'browser_monitoring.debug']}",
                                     "JS agent loader version: #{NewRelic::Agent.config[:'browser_monitoring.loader_version']}")

        if !NewRelic::Agent.config[:'rum.enabled']
          NewRelic::Agent.logger.debug("Real User Monitoring is disabled for this agent. Edit your configuration to change this.")
        end
      end

      def enabled?
        Agent.config[:'rum.enabled'] && !!Agent.config[:beacon]
      end

      def obfuscator
        @obfuscator ||= NewRelic::Agent::Obfuscator.new(NewRelic::Agent.config[:license_key], RUM_KEY_LENGTH)
      end

      def current_transaction
        NewRelic::Agent::TransactionState.get.transaction
      end

      def insert_js?
        if !enabled?
          ::NewRelic::Agent.logger.log_once(:debug, :js_agent_disabled,
                                            "JS agent instrumentation is disabled.")
          false
        elsif missing_config?(:js_agent_loader)
          ::NewRelic::Agent.logger.log_once(:debug, :missing_js_agent_loader,
                                            "Missing :js_agent_loader. Skipping browser instrumentation.")
          false
        elsif missing_config?(:beacon)
          ::NewRelic::Agent.logger.log_once(:debug, :missing_beacon,
                                            "Beacon configuration not received (yet?). Skipping browser instrumentation.")
          false
        elsif missing_config?(:browser_key)
          ::NewRelic::Agent.logger.log_once(:debug, :missing_browser_key,
                                            "Browser key is not set. Skipping browser instrumentation.")
          false
        elsif !current_transaction
          ::NewRelic::Agent.logger.debug "Not in transaction. Skipping browser instrumentation."
          false
        elsif !::NewRelic::Agent.is_transaction_traced?
          ::NewRelic::Agent.logger.debug "Transaction is not traced. Skipping browser instrumentation."
          false
        elsif !::NewRelic::Agent.is_execution_traced?
          ::NewRelic::Agent.logger.debug "Execution is not traced. Skipping browser instrumentation."
          false
        elsif ::NewRelic::Agent::TransactionState.get.request_ignore_enduser
          ::NewRelic::Agent.logger.debug "Ignore end user for this transaction is set. Skipping browser instrumentation."
          false
        else
          true
        end
      end

      def missing_config?(key)
        value = NewRelic::Agent.config[key]
        value.nil? || value.empty?
      end

      def browser_timing_header
        return "" unless insert_js?
        browser_timing_config + browser_timing_loader
      end

      # NOTE: Internal prototyping often overrides this, so leave name stable!
      def browser_timing_loader
        html_safe_if_needed("\n<script type=\"text/javascript\">#{Agent.config[:js_agent_loader]}</script>")
      end

      # NOTE: Internal prototyping often overrides this, so leave name stable!
      def browser_timing_config
        NewRelic::Agent::Transaction.freeze_name
        data = data_for_js_agent
        json = NewRelic.json_dump(data)
        html_safe_if_needed("\n<script type=\"text/javascript\">window.NREUM||(NREUM={});NREUM.info=#{json}</script>")
      end

      BEACON_KEY           = "beacon".freeze
      ERROR_BEACON_KEY     = "errorBeacon".freeze
      LICENSE_KEY_KEY      = "licenseKey".freeze
      APPLICATIONID_KEY    = "applicationID".freeze
      TRANSACTION_NAME_KEY = "transactionName".freeze
      QUEUE_TIME_KEY       = "queueTime".freeze
      APPLICATION_TIME_KEY = "applicationTime".freeze
      TT_GUID_KEY          = "ttGuid".freeze
      AGENT_TOKEN_KEY      = "agentToken".freeze
      AGENT_KEY            = "agent".freeze
      EXTRA_KEY            = "extra".freeze
      SSL_FOR_HTTP_KEY     = "sslForHttp".freeze

      # NOTE: Internal prototyping may override this, so leave name stable!
      def data_for_js_agent
        state = NewRelic::Agent::TransactionState.get
        timings = state.timings

        data = {
          BEACON_KEY           => NewRelic::Agent.config[:beacon],
          ERROR_BEACON_KEY     => NewRelic::Agent.config[:error_beacon],
          LICENSE_KEY_KEY      => NewRelic::Agent.config[:browser_key],
          APPLICATIONID_KEY    => NewRelic::Agent.config[:application_id],
          TRANSACTION_NAME_KEY => obfuscator.obfuscate(timings.transaction_name_or_unknown),
          QUEUE_TIME_KEY       => timings.queue_time_in_millis,
          APPLICATION_TIME_KEY => timings.app_time_in_millis,
          TT_GUID_KEY          => state.request_guid_to_include,
          AGENT_TOKEN_KEY      => state.request_token,
          AGENT_KEY            => NewRelic::Agent.config[:js_agent_file],
          EXTRA_KEY            => obfuscator.obfuscate(formatted_extra_parameter_for_js_agent)
        }
        add_ssl_for_http(data)

        data
      end

      def add_ssl_for_http(data)
        ssl_for_http = NewRelic::Agent.config[:'browser_monitoring.ssl_for_http']
        unless ssl_for_http.nil?
          data[SSL_FOR_HTTP_KEY] = ssl_for_http
        end
      end

      # NOTE: Internal prototyping may override this, so leave name stable!
      def data_for_js_agent_extra_parameter
        return {} unless include_custom_parameters_in_extra?
        current_transaction.custom_parameters.dup
      end

      def include_custom_parameters_in_extra?
        current_transaction &&
          NewRelic::Agent.config[:'analytics_events.enabled'] &&
          NewRelic::Agent.config[:'capture_attributes.page_view_events']
      end

      def formatted_extra_parameter_for_js_agent
        format_extra_data(data_for_js_agent_extra_parameter)
      end

      # Format the props using semicolon separated pairs separated by '=':
      #   product=pro;user=bill@microsoft.com
      def format_extra_data(extra_props)
        event_params(extra_props).
          map {|k,v| format_pair(k, v)}.
          join(';')
      end

      def format_pair(key, value)
        key = escape_special_characters(key)
        value = format_value(value)
        "#{key}=#{value}"
      end

      def escape_special_characters(string)
        string.to_s.tr("\";=", "':-" )
      end

      def format_value(v)
        v = "##{v}" if v.is_a?(Numeric)
        escape_special_characters(v)
      end

      def html_safe_if_needed(string)
        string = string.html_safe if string.respond_to?(:html_safe)
        string
      end
    end
  end
end
