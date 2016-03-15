require 'json'

module SimpleQueue
  module Queue
    class FileSystem
      SUFFIX_FILE_NAME = '.simplequeue.queue'.freeze

      def initialize(name, path = Dir.pwd)
        @name = "#{name.downcase}#{SUFFIX_FILE_NAME}".freeze
        @path = path ? File.join(path, @name) : @name
        FileUtils.touch(@path)

        @semaphore = Concurrent::Semaphore.new(1)
        @queue = Concurrent::Array.new
      end

      def enqueue(worker, *args)
        payload = { worker: worker, args: Array(args) }.to_json

        with_queue_lock do
          queue.unshift(payload).tap { save_queue }.first
        end
      end

      def next
        content = with_queue_lock do
          queue.pop.tap { save_queue }
        end

        content && JSON.parse(content)
      end

      def increment(counter)
        with_file_lock(counter_file(counter)) do |file|
          redis.incr(counter_file(counter))
        end
      end

      def reset(counter)
        with_file_lock(counter_file(counter)) do
          redis.set(counter_file(counter), 0)
        end
      end

      def fetch_counter(counter)
        with_file_lock(counter_file(counter)) do
          redis.get(counter_file(counter))
        end
      end

      def all
        with_queue_lock { queue.to_a }
      end

      def size
        with_queue_lock { queue.size }
      end

      private

      def with_lock
        Timeout.timeout(600) do
          while true
            break if semaphore.try_acquire
            sleep(0.1)
          end
        end

        with_queue_lock(file) { yield }
      ensure
        semaphore.release
      end

      attr_reader :path, :semaphore, :queue

      def with_queue_lock
        with_file_lock(path) do |file|
          load_queue(file)
          yield
        end #.tap { queue.clear }
      end

      # Lock file
      #
      # Reference: http://douglasfshearer.com/2011/02/13/threadsafe-file-consistency-in-ruby.html
      #
      def with_file_lock(file)
        result = nil
        File.open(file, 'r+') do |f|
          f.flock(File::LOCK_EX)
          result = yield(file)
          f.flock(File::LOCK_UN)
        end
        result
      end

      def load_queue(file)
        @queue = Concurrent::Array.new(load_file(file))
      end

      def save_queue(file)
        File.open(path, 'w') { |file| file.write(queue.to_json) }
      end

      def load_file(file)
        content = file.read.strip
        (content.presence && JSON.parse(content)) || []
      end

      def counter_file(type)
        "#{path}.#{type.downcase}.count".freeze
      end
    end
  end
end
