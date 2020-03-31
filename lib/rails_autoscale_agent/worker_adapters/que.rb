# frozen_string_literal: true

require 'rails_autoscale_agent/logger'
require 'time'

module WorkerAdapters
  class Que
    include RailsAutoscaleAgent::Logger
    include Singleton

    DEFAULT_QUEUES = ['default']

    class << self
      attr_accessor :queues
    end

    def initialize
      self.class.queues = DEFAULT_QUEUES
    end

    def enabled?
      defined? ::Que
    end

    def collect!(store)
      log_msg = String.new('Que latency ')
      t = Time.now

      # Ignore failed jobs (they skew latency measurement due to the original run_at)
      sql = 'SELECT queue, min(run_at) FROM que_jobs WHERE error_count = 0 GROUP BY queue'
      run_at_by_queue = Hash[ActiveRecord::Base.connection.select_rows(sql)]
      self.class.queues |= run_at_by_queue.keys

      self.class.queues.each do |queue|
        run_at = run_at_by_queue[queue]
        run_at = Time.parse(run_at) if run_at.is_a?(String)
        latency_ms = run_at ? ((t - run_at)*1000).ceil : 0
        store.push latency_ms, t, queue
        log_msg << "#{queue}=#{latency_ms} "
      end

      logger.debug log_msg
    end
  end
end
