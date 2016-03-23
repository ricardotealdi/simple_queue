module SimpleQueue
  module Storage
    class Base
      def enqueue(queue, payload)
        raise NotImplementedError
      end

      def next(queue)
        raise NotImplementedError
      end

      def queue_size(queue)
        raise NotImplementedError
      end

      def increment(counter)
        raise NotImplementedError
      end

      def stats
        raise NotImplementedError
      end
    end

    class Redis < Base
      def initialize(connection_pool)
        @connection_pool = connection_pool
        @stats_helper = Stats.new(connection_pool)
      end

      def enqueue(queue, payload)
        queue_helper(queue).add(payload)
      end

      def next(queue)
        queue_helper(queue).pop
      end

      def queue_size(queue)
        queue_helper(queue).size
      end

      def increment(counter)
        stats_helper.increment(counter)
      end

      def stats
        stats_helper.all
      end

      private

      attr_reader :connection_pool, :stats_helper

      def queue_helper(queue)
        Queue.new(queue, connection_pool)
      end

      module Connection
        PREFIX_KEY = 'simpleq:'.freeze

        def with_connection
          connection_pool.with { |redis| mutex.synchronize { yield(redis) } }
        end
      end

      class Queue
        include Connection

        PREFIX_QUEUE_KEY = "#{PREFIX_KEY}queue:".freeze

        def initialize(queue, connection_pool)
          @name = "#{PREFIX_QUEUE_KEY}#{queue.downcase}".freeze
          @connection_pool = connection_pool
          @mutex = Mutex.new
        end

        def add(payload)
          with_connection { |redis| redis.lpush(name, payload) }
        end

        def pop
          with_connection { |redis| redis.rpop(name) }
        end

        def size
          with_connection { |redis| redis.llen(name) }
        end

        private

        attr_reader :name, :connection_pool, :mutex
      end

      class Stats
        include Connection

        COUNTER_KEY = "#{PREFIX_KEY}counters".freeze

        def initialize(connection_pool)
          @connection_pool = connection_pool
          @mutex = Mutex.new
        end

        def increment(counter)
          with_connection { |redis| redis.hincrby(COUNTER_KEY, counter, 1) }
        end

        def fetch(counter)
          with_connection do |redis|
            (redis.hget(COUNTER_KEY, counter) || 0).to_i
          end
        end

        def all
          with_connection do |redis|
            redis.hgetall(COUNTER_KEY).reduce({}) do |hash, (k, v)|
              hash[k.to_sym] = v.to_i
              hash
            end
          end
        end

        private

        attr_reader :connection_pool, :mutex
      end
    end
  end
end
