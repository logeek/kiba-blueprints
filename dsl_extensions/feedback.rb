require 'kiba-common/dsl_extensions/logger'

module Kiba
  module Blueprints
    module DSLExtensions
      module Feedback
        # NOTE: just a little helper to easily inject some stats in a given job
        def setup_feedback(klass, feedback_freq: 25_000)
          extend Kiba::Common::DSLExtensions::Logger

          pre_process do
            @processed_rows_count = 0
            @start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            logger.info "Starting #{klass}"
          end
          transform do |r|
            @processed_rows_count += 1
            if @processed_rows_count % feedback_freq == 0
              logger.info "Processing #{feedback_freq} rows..."
            end
            r
          end
          post_process do
            time_taken = Process.clock_gettime(Process::CLOCK_MONOTONIC) - @start_time
            logger.info "Total run took #{time_taken.round(2)} seconds"
            logger.info "Rows processed: #{@processed_rows_count}"
            rows_per_second = (@processed_rows_count / time_taken).round(1)
            logger.info "Throughput: #{rows_per_second} rows per second"
          end
        end
      end
    end
  end
end
