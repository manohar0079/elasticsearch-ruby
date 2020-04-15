require "ansi/core"
require "logger"
require "patron"
require "pathname"
require "oj"

require_relative "runner/runner"

puts "Running benchmarks for elasticsearch-ruby@#{Elasticsearch::VERSION}".ansi(:bold,:underline)

config = {
  "ELASTICSEARCH_TARGET_URL"     => "",
  "ELASTICSEARCH_REPORT_URL"     => "",
  "DATA_SOURCE"                  => "",
  "BUILD_ID"                     => "",
  "TARGET_SERVICE_TYPE"          => "",
  "TARGET_SERVICE_NAME"          => "",
  "TARGET_SERVICE_VERSION"       => "",
  "TARGET_SERVICE_OS_FAMILY"     => "",
  "CLIENT_BRANCH"                => "",
  "CLIENT_COMMIT"                => "",
  "CLIENT_BENCHMARK_ENVIRONMENT" => ""
}

missing_keys = []

config.keys.each do |key|
  if ENV[key] && !ENV[key].to_s.empty?
    config[key] = ENV[key]
  else
    missing_keys << key
  end
end

unless missing_keys.empty?
puts "ERROR: Required environment variables [#{missing_keys.join(',')}] missing".ansi(:bold, :red)
  exit(1)
end

start = Time.now.utc

runner_client = Elasticsearch::Client.new(url: config["ELASTICSEARCH_TARGET_URL"])
report_client = Elasticsearch::Client.new(
  url: config["ELASTICSEARCH_REPORT_URL"],
  request_timeout: 5*60,
  retry_on_failure: 10
)
if ENV['DEBUG']
  logger = Logger.new(STDOUT)
  logger.level = Logger::INFO
  logger.formatter = proc { |s, d, p, m| "#{m}\n".ansi(:faint) }

  runner_client.transport.logger = logger
  report_client.transport.logger = logger
end

runner  = Runner::Runner.new \
  build_id: config['BUILD_ID'],
  environment: config['CLIENT_BENCHMARK_ENVIRONMENT'],
  category: ENV['CLIENT_BENCHMARK_CATEGORY'].to_s,
  runner_client: runner_client,
  report_client: report_client,
  target: {
    service: {
      type: config['TARGET_SERVICE_TYPE'],
      name: config['TARGET_SERVICE_NAME'],
      version: config['TARGET_SERVICE_VERSION'],
      git: {
        branch: ENV['TARGET_SERVICE_GIT_BRANCH'],
        commit: ENV['TARGET_SERVICE_GIT_COMMIT']
      }
    },
    os: {
      family: config['TARGET_SERVICE_OS_FAMILY']
    }
  },
  runner: {
    service: {
      git: {
        branch: config['CLIENT_BRANCH'],
        commit: config['CLIENT_COMMIT']
      }
    }
  }

data_path = Pathname(config["DATA_SOURCE"])
unless data_path.exist?
  puts "ERROR: Data source at [#{data_path}] not found".ansi(:bold, :red)
  exit(1)
end

# ----- Run benchmarks --------------------------------------------------------
[
  { action: 'ping', warmups: 0,   repetitions: 1000, block: Proc.new { |n, runner| runner.runner_client.ping } },
  { action: 'info', warmups: 0,   repetitions: 1000, block: Proc.new { |n, runner| runner.runner_client.info } },
  { action: 'get',  warmups: 100, repetitions: 1000,
    setup: Proc.new do |runner|
      runner.runner_client.indices.delete(index: 'test-bench-get', ignore: 404)
      runner.runner_client.index index: 'test-bench-get', id: '1', body: { title: 'Test' }
      runner.runner_client.indices.refresh index: 'test-bench-get'
    end,
    block: Proc.new do |n, runner|
      response = runner.runner_client.get index: 'test-bench-get', id: '1'
      raise RuntimeError.new("Incorrect data: #{response}") unless response["_source"]["title"] == "Test"
    end
  },
  { action: 'index',  warmups: 1, repetitions: 1000,
    setup: Proc.new do |runner|
      runner.runner_client.indices.delete(index: 'test-bench-index', ignore: 404)
      runner.runner_client.indices.create(index: 'test-bench-index')
    end,
    block: Proc.new do |n, runner|
      doc_path = data_path.join('small/document.json')
      raise RuntimeError.new("Document at #{doc_path} not found") unless doc_path.exist?
      response = runner.runner_client.index index: 'test-bench-index', id: "%04d-%04d" % [n, rand(1..1000)], body: doc_path.open.read
      raise RuntimeError.new("Incorrect response: #{response}") unless response["result"] == "created"
    end
  }
].each do |b|
  next unless ENV['FILTER'].nil? or ENV['FILTER'].include? b[:action]

  runner.setup(&b[:setup]) if b[:setup]

  result = runner.measure(
    action: b[:action],
    warmups: b[:warmups],
    repetitions: b[:repetitions],
    &b[:block]).run!

  puts "  " +
       "[#{b[:action]}] ".ljust(16) +
       "#{b[:repetitions]}x ".ljust(10) +
       "mean=".ansi(:faint) +
       "#{coll = runner.stats.map(&:duration); ((coll.sum / coll.size.to_f)/1e+6).round}ms " +
       "runner=".ansi(:faint)+
       "#{runner.stats.any? { |s| s.outcome == 'failure' } ? 'failure' : 'success'  } ".ansi( runner.stats.none? { |s| s.outcome == 'failure' } ? :green : :red ) +
       "report=".ansi(:faint)+
       "#{result ? 'success' : 'failure' }".ansi( result ? :green : :red )
end
# -----------------------------------------------------------------------------

puts "Finished in #{(Time.mktime(0)+(Time.now.utc-start)).strftime("%H:%M:%S")}".ansi(:underline)
