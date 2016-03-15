require 'json'

module SimpleQueue
  module Queue
    class FileSystem
      def initialize(path)
        FileUtils.touch(path)
        @path = path
        @semaphore = Concurrent::Semaphore.new(1)
        @queue = Concurrent::Array.new
      end

      def enqueue(worker, *args)
        with_lock do
          queue.unshift(
            { worker: worker, args: Array(args) }.to_json
          ).tap { save_queue }
        end
      end

      def next
        with_lock do
          content = queue.pop.tap { save_queue }
          content && JSON.parse(content)
        end
      end

      def all
        with_lock { queue.to_a }
      end

      def size
        with_lock { queue.size }
      end

      private

      def with_lock
        Timeout.timeout(600) do
          while true
            break if semaphore.try_acquire
            sleep(0.1)
          end
        end

        with_file_lock { yield }
      ensure
        semaphore.release
      end

      attr_reader :path, :semaphore, :queue

      # Lock file
      #
      # Reference: http://douglasfshearer.com/2011/02/13/threadsafe-file-consistency-in-ruby.html
      #
      def with_file_lock
        result = nil
        File.open(path, 'r+') do |f|
          f.flock(File::LOCK_EX)
          load_queue
          result = yield
          f.flock(File::LOCK_UN)
          queue.clear
        end
        result
      end

      def load_queue
        @queue = Concurrent::Array.new(load_file)
      end

      def save_queue
        File.open(path, 'w') { |file| file.write(queue.to_json) }
      end

      def load_file
        content = File.open(path, 'r').read.strip
        (content.presence && JSON.parse(content)) || []
      end
    end
  end
end

