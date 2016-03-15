module SimpleQueue
  module Queue
    class Directory
      module Generators
        class Path
          extend(Forwardable)

          TEMPORARY_SUFFIX = '.tmp'.freeze

          def initialize(path, granularity = 60)
            @path = path
            @dir_generator = Dir.new(granularity)
            @file_generator = File.new
          end

          def tempfile
            "#{realfile}#{TEMPORARY_SUFFIX}"
          end

          def filename
            next_file
          end

          private

          attr_reader :path, :dir_generator, :file_generator

          def_delegators :dir_generator, :next_dir
          def_delegators :file_generator, :next_file

          def base_dir
            ::File.join(path, next_dir)
          end

          def realfile
            ::File.join(base_dir, next_file)
          end
        end
      end
    end
  end
end
