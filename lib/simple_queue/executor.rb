module SimpleQueue
  class Executor
    def initialize(pool, logger, &execution_proc)
      @pool = pool
      @logger = logger
      @execution_proc = execution_proc
    end

    def execute
      pool.post do
        return unless pool.running?
        begin
          execution_proc.call
        rescue => e
          logger.error("#{e.class}: #{e.message} - #{e.backtrace.take(5)}")
        end
      end

      true
    rescue Concurrent::RejectedExecutionError
      false
    end


    private

    attr_reader :pool, :logger, :execution_proc
  end
end
