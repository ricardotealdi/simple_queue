module SimpleQueue
  module Queue
    class Directory
      module Generators
        class Dir
          def initialize(granularity = 60)
            @granularity = granularity
          end

          def next_dir
            now = Time.now.to_i
            now -= now % granularity if granularity > 1

            "%08x" % now
          end

          private

          attr_reader :granularity
        end
      end
    end
  end
end
