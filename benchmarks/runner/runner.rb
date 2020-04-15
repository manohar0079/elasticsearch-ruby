require "ostruct"
require "time"
require "rbconfig"

require "ansi/core"
require "elasticsearch"

##
# Module Runner contains components for end-to-end benchmarking of the Elasticsearch client.
#
module Runner
  ##
  # Stats represents the measured statistics.
  #
  class Stats < OpenStruct; end

  ##
  # ReportError represents an exception ocurring during reporting the results.
  #
  class ReportError < StandardError; end

  ##
  # SetupError represents an exception occuring during operation setup.
  #
  class SetupError < StandardError; end

  ##
  # WarmupError represents an exception occuring during operation warmup.
  #
  class WarmupError < StandardError; end

  ##
  # The bulk size for reporting results.
  #
  BULK_BATCH_SIZE = 1000

  ##
  # The index name for reporting results.
  #
  INDEX_NAME="metrics-intake-#{Time.now.strftime("%Y-%m")}"

  ##
  # Runner represents a benchmarking runner.
  #
  # It is initialized with two Elasticsearch clients, one for running the benchmarks,
  # another one for reporting the results.
  #
  # Use the {#measure} method for adding a block which is executed and measured.
  #
  class Runner
    attr_reader :stats, :runner_client, :report_client, :warmups, :repetitions

    ##
    # @param runner_client [Elasticsearch::Client] The client for executing the measured operations.
    # @param report_client [Elasticsearch::Client] The client for storing the results.
    #
    def initialize(build_id:, category:, environment:, runner_client:, report_client:, target:, runner:)
      @action = ''
      @stats = []
      @warmups = 0
      @repetitions = 0
      @run_duration = 0

      @build_id = build_id
      @category = category
      @environment = environment
      @runner_client = runner_client
      @report_client = report_client
      @target_config = target
      @runner_config = runner
    end

    ##
    # Executes the measured block, capturing statistics, and reports the results.
    #
    # @return [Boolean]
    # @raise [ReportError]
    #
    def run!
      @stats = []

      # Run setup code
      begin
        @setup.arity < 1 ? self.instance_eval(&@setup) : @setup.call(self) if @setup
      rescue StandardError => e
        raise SetupError.new(e.inspect)
      end

      # Run warmups
      begin
        @warmups.times do |n|
          @measure.arity < 1 ? self.instance_eval(&@measure) : @measure.call(n, self) if @measure
        end
      rescue StandardError => e
        raise WarmupError.new(e.inspect)
      end

      # Run measured repetitions
      #
      # Cf. https://blog.dnsimple.com/2018/03/elapsed-time-with-ruby-the-right-way/
      @repetitions.times do |n|
        stat = Stats.new(start: Time.now.utc)
        start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        begin
          result = @measure.arity < 1 ? result = self.instance_eval(&@measure) : result = @measure.call(n, self) if @measure
          if result == false
            stat.outcome = "failure"
          else
            stat.outcome = "success"
          end
        rescue StandardError => e
          stat.outcome = "failure"
        ensure
          stat.duration = ((Process.clock_gettime(Process::CLOCK_MONOTONIC)-start) * 1e+9 ).to_i
          @stats << stat
        end
      end

      # Report results
      begin
        __report
      rescue StandardError => e
        puts "ERROR: #{e.inspect}"
        return false
      end

      return true
    end

    ##
    # Configure a setup for the measure operation.
    #
    # @return [self]
    #
    def setup &block
      @setup = block
      return self
    end

    ##
    # Configure the measure operation.
    #
    # @param action      [String] A human-readable name of the operation.
    # @param warmups     [Number] The number of warmup operations. (Default: 0)
    # @param repetitions [Number] The number of warmup operations. (Default: 0)
    # @param block       [Block]  The measure operation definition.
    #
    # @return [self]
    #
    def measure(action:, warmups:, repetitions:, &block)
      @action = action
      @warmups = warmups
      @repetitions = repetitions
      @measure = block
      return self
    end

    ##
    # Stores the result in the reporting cluster.
    #
    # @api private
    #
    def __report
      @stats.each_slice(BULK_BATCH_SIZE) do |slice|
        payload = slice.map do |s|
          { index: {
              data: {
                :'@timestamp' => s.start.iso8601,
                labels: { client: 'elasticsearch-ruby', environment: @environment.to_s },
                tags: ['bench', 'elasticsearch-ruby'],
                event: { action: @action, duration: s.duration },
                benchmark: {
                  build_id: @build_id,
                  environment: @environment.to_s,
                  category: @category.to_s,
                  repetitions: @repetitions,
                  runner: {
                    service: @runner_config[:service].merge({
                      type: 'client',
                      name: 'elasticsearch-ruby',
                      version: Elasticsearch::VERSION
                    }),
                    runtime: {
                      name: 'ruby', version: RbConfig::CONFIG['ruby_version']
                    },
                    os: {
                      family: RbConfig::CONFIG['host_os'].split('_').first[/[a-z]+/i].downcase
                    }
                  },
                  target: @target_config
                }
              }
            }
          }
        end

        begin

        rescue Elasticsearch::Transport::Transport::Error => e
          puts "ERROR: #{e.inspect}"
          raise e
        end

        response = @report_client.bulk index: INDEX_NAME, body: payload
        if response['errors'] || response['items'].any? { |i| i.values.first['status'] > 201 }
          raise ReportError.new("Error saving benchmark results to report cluster")
        end
      end
    end
  end
end
