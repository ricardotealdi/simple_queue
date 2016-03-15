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
        with_connection do |redis|
          redis.lpush(name, { worker: worker_class, args: Array(args) }.to_json)
        end
      end

      def next
        with_connection do |redis|
          content = redis.rpop(name)
          content && JSON.parse(content)
        end
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
    end
  end
end
