require_relative 'directory/generators'

module SimpleQueue
  module Queue
    class Directory
      DIRECTORY_PATTERN = /^([0-9a-f]{8})$/.freeze
      ELEMENT_PATTERN = /^([0-9a-f]{14})$/.freeze
      LOCKED_SUFFIX = '.lck'.freeze

      def initialize(
        name, path: Dir.pwd, granularity: 60, logger: ::Logger.new(STDOUT)
      )
        @name = name.to_s.freeze
        @path = File.join(path, name).to_s.freeze
        FileUtils.mkdir_p(@path)
        @path_generator = Generators::Path.new(@path, granularity)
        @mutex = Mutex.new
        @logger = logger
        @counters = Hash.new(0)
      end


      def enqueue(worker, *args)
        payload = { worker: worker, args: args }.to_json

        save(payload)

        true
      end

      def next
        content = nil
        mutex.synchronize do
          while candidate = all.peek
            break if lock(candidate)
            all.next
          end

          content = (candidate && File.read(candidate))
          remove(candidate)
        end

        content && JSON.parse(content)
      rescue StopIteration
        return nil
      end

      def all
        Enumerator.new do |y|
          queue_dirs.each do |dir|
            Dir.glob(File.join(dir, '**')).each do |file|
              File.file?(file) &&
                ELEMENT_PATTERN.match(File.basename(file)) &&
                y.yield(file)
            end
          end
        end
      end

      def count
        all.count
      end

      def increment(counter)
        with_file_lock(counter_file_name(counter)) do |file|
          (file.gets.to_i + 1).tap do |value|
            file.rewind
            file.puts(value)
          end
        end
      end

      def fetch_counter(counter)
        with_file_lock(counter_file_name(counter)) do |file|
          file.gets.to_i
        end
      end

      private

      attr_reader(:name, :path, :path_generator, :mutex, :logger, :counters)

      def first
        all.first
      end

      def temp_path
        path_generator.tempfile
      end

      def next_file
        path_generator.next_file
      end

      def fetch_lock_name(file_name)
        "#{file_name}#{LOCKED_SUFFIX}"
      end

      def queue_dirs
        Enumerator.new do |y|
          Dir.glob(File.join(path, '**')).select do |dir|
            File.directory?(dir) &&
              DIRECTORY_PATTERN.match(File.basename(dir)) &&
              y.yield(dir)
          end
        end
      end

      def save(payload)
        mutex.synchronize do
          temp = save_on_temp(payload)
          move_to_real(temp)
        end
      rescue Errno::ENOENT => e
        logger.debug "#{__method__}: #{e.inspect} #{e.backtrace.take(1)}"
        retry
      end

      def save_on_temp(data)
        begin
          temp = temp_path
        end until !File.exist?(temp)
        File.open(temp, 'w') { |file| file.write(data) }
        temp
      rescue Errno::ENOENT, Errno::ENOTDIR => e
        logger.debug "#{__method__}: #{e.inspect}"
        dir = File.dirname(temp)
        FileUtils.mkdir_p(dir)
        retry
      end

      def move_to_real(temp)
        dir = File.dirname(temp)

        begin
          new = File.join(dir, next_file)
          File.link(temp, new)
        rescue Errno::EEXIST => e
          logger.debug "#{__method__}: #{e.inspect}"
          retry
        end

        File.unlink(temp) rescue Errno::ENOENT nil

        new
      end

      def lock(file_name)
        lock_file_name = fetch_lock_name(file_name)

        begin
          File.link(file_name, lock_file_name)
        rescue Errno::ENOENT, Errno::EEXIST => e
          logger.debug "#{__method__}: #{e.inspect} #{e.backtrace.take(1)}"
          return false
        end

        now = Time.now

        begin
          File.utime(now, now, file_name)
        rescue Errno::ENOENT => e
          logger.debug "#{__method__}: #{e.inspect} #{e.backtrace.take(1)}"

          File.unlink(lock_file_name) rescue Errno::ENOENT nil
          return false
        end

        true
      end

      def remove(file_name)
        lock_file_name = fetch_lock_name(file_name)
        File.unlink(file_name)
        File.unlink(lock_file_name)
      end

      def counter_file_name(counter)
        File.join(path, counter.to_s.downcase)
      end

      def _counter(counter)
        counter_file = counter_file_name(counter)

        with_file_lock(counter_file_name) do |file|
          value = file.gets.to_i
          file.rewind
          file.puts(value.next)
        end
      end

      # Lock file
      #
      # Reference: http://douglasfshearer.com/2011/02/13/threadsafe-file-consistency-in-ruby.html
      #
      def with_file_lock(file)
        result = nil
        FileUtils.touch(file)
        File.open(file, 'r+') do |f|
          f.flock(File::LOCK_EX)
          mutex.synchronize do
            result = yield(f)
          end
          f.flock(File::LOCK_UN)
        end
        result
      end
    end
  end
end
