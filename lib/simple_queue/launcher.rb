module SimpleQueue
  class Launcher
    def initialize(opts = {})
      min_threads = opts.fetch(
        :min_threads, ENV.fetch('SIMPLE_QUEUE_MIN_THREADS', 0)
      ).to_i

      max_threads = opts.fetch(
        :max_threads, ENV.fetch('SIMPLE_QUEUE_MAX_THREADS', 25)
      ).to_i

      @logger = SimpleQueue::Logger.new(
        opts.fetch(:logger, ::Logger.new(STDOUT))
      )

      @queue = opts.fetch(:queue)

      @pool = Concurrent::ThreadPoolExecutor.new(
        min_threads: min_threads, max_threads: max_threads
      )
    end

    def run
      Consumer.new(pool, queue, logger).consume2
    rescue Interrupt => e
      log "Interrupt received"
      log "Shutting down..."
      pool.shutdown
      pool.kill unless pool.wait_for_termination(60)
      log "Shutted down!"
    end

    private

    attr_reader :pool, :logger, :queue

    def log(message)
      logger.tagged(self.class) { logger.info(message) }
    end
  end
end
