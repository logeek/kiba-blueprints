module Kiba
  module Blueprints
    module DSLExtensions
      module Feedback
        # NOTE: just a little helper to easily inject some stats in a given job
        def setup_feedback(klass)
          pre_process do
            @processed_rows_count = 0
            @start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            logger.info "Starting #{klass}"
          end
          transform do |r|
            @processed_rows_count += 1
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
