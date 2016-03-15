module SimpleQueue
  module Queue
    class InMemory
      def initialize
        @queue = Concurrent::Array.new
      end

      def enqueue(element)
        queue.unshift(element)
      end

      def next
        queue.pop
      end

      def all
        queue.to_a
      end

      def size
        queue.size
      end

      private

      attr_reader :queue
    end
  end
end
