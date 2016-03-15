require 'redis'
require 'connection_pool'

module SimpleQueue
  module Queue
    class Redis
      PREFIX_QUEUE_KEY = 'simplequeue:queue:'.freeze

      def initialize(name, connection_pool)
        @name = "#{PREFIX_QUEUE_KEY}#{name.downcase}".freeze
        @connection_pool = connection_pool
        @mutex = Mutex.new
      end

      def enqueue(worker_class, *args)
        payload = { worker: worker_class, args: Array(args) }.to_json

        with_connection do |redis|
          redis.lpush(name, payload)
        end
      end

      def next
        content = with_connection do |redis|
          redis.rpop(name)
        end

        content && JSON.parse(content)
      end

      def increment(counter)
        with_connection { |redis| redis.incr(counter_key(counter)) }
      end

      def reset(counter)
        with_connection { |redis| redis.set(counter_key(counter), 0) }
      end

      def fetch_counter(counter)
        with_connection { |redis| redis.get(counter_key(counter)) }
      end

      def all
        with_connection { |redis| redis.lrange(name, 0, -1) }
      end

      def size
        with_connection { |redis| redis.llen(name) }
      end

      private

      attr_reader :name, :connection_pool, :mutex

      def with_connection
        connection_pool.with { |redis| mutex.synchronize { yield(redis) } }
      end

      def counter_key(type)
        "#{name}:#{type.downcase}:count".freeze
      end
    end
  end
end
