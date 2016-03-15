module SimpleQueue
  module Queue
    class Directory
      module Generators
        class File
          def next_file
            now = Time.now.to_f
            secs = now.to_i
            msecs = ((now - secs) * 1000000).to_i

            ("%08x%05x%01x" % [secs, msecs, rand(16)])[0..13]
          end
        end
      end
    end
  end
end
