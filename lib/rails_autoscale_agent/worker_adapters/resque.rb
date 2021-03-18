# frozen_string_literal: true

require 'rails_autoscale_agent/logger'

module RailsAutoscaleAgent
  module WorkerAdapters
    class Resque
      include RailsAutoscaleAgent::Logger
      include Singleton

      attr_writer :queues

      def queues
        @queues ||= ['default']
      end

      def enabled?
        require 'resque'

        log_msg = String.new("Resque enabled")
        log_msg << " with long-running job support" if track_long_running_jobs?
        logger.info log_msg

        true
      rescue LoadError
        false
      end

      def collect!(store)
        log_msg = String.new
        current_queues = ::Resque.queues

        # Don't collect worker metrics if there are unreasonable number of queues
        if current_queues.size > 50
          logger.debug "Skipping Resque metrics - #{current_queues.size} queues"
          return
        end

        if track_long_running_jobs?
          busy_counts = Hash.new { |h,k| h[k] = 0}

          ::Resque.working.each do |worker|
            if !worker.idle? && job = worker.job
              busy_counts[job['queue']] += 1
            end
          end
        end

        # Ensure we continue to collect metrics for known queue names, even when nothing is
        # enqueued at the time. Without this, it will appears that the agent is no longer reporting.
        self.queues |= current_queues

        queues.each do |queue|
          next if queue.nil? || queue.empty?
          depth = ::Resque.size(queue)
          store.push depth, Time.now, queue, :qd
          log_msg << "resque-qd.#{queue}=#{depth} "

          if track_long_running_jobs?
            busy_count = busy_counts[queue]
            store.push busy_count, Time.now, queue, :busy
            log_msg << "resque-busy.#{queue}=#{busy_count} "
          end
        end

        logger.debug log_msg
      end

      private

      def track_long_running_jobs?
        Config.instance.track_long_running_jobs
      end
    end
  end
end
