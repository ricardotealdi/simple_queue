module SimpleQueue
  class Consumer
    def initialize(pool, queue, logger)
      @pool = pool
      @queue = queue
      @logger = logger
      @i = 0
    end

    def consume
      (1..pool.max_length).map do |n|
        Concurrent::Future.new(executor: pool) do
          while true
            if !pool.running?
              logger.info "Stopping worker #{n}"
              break
            end

            execute || sleep(1)
          end
        end
      end.map.with_index(1) do |it, n|
        logger.info "Starting worker #{n}"
        it.execute
      end.map(&:value)
    end

    private

    attr_reader :pool, :logger, :queue

    def execute
      current = queue.next

      if current
        worker = begin
                   current.fetch('worker').constantize
                 rescue NoMethodError
                   nil
                 end

        return false unless worker

        args = current.fetch('args')
        logger.info "i:#{@i += 1}, worker:#{worker}, value: #{args} processing"
        queue.increment :processed
        begin
          worker.new(logger).perform(*args)
          queue.increment :success
          return true
        rescue => e
          queue.increment :error
          queue.enqueue(worker, *args)
          logger.error "Error: #{e.class}: #{e.message} #{e.backtrace.take(5)}"
        end
      end

      false
    end
  end
end

class Worker
  def initialize(logger)
    @logger = logger
    freeze
  end

  def perform(id)
    sleep((rand(5) + 1) * 0.2)
    1 / rand(100)
    logger.info "#{id} executed"
  end

  attr_reader :logger
end
