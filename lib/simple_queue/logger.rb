module SimpleQueue
  class Logger
    def initialize(logger = ::Logger.new(STDOUT))
      logger.formatter = ::Logger::Formatter.new unless logger.formatter
      @logger = ActiveSupport::TaggedLogging.new(logger)
    end

    %w(debug info error warn fatal).each do |severity|
      define_method(severity) do |*args, &block|
        logger.tagged(Thread.current.object_id) do
          if block
            logger.public_send(severity, *args, &block)
          else
            logger.public_send(severity, *args)
          end
        end
      end
    end

    def tagged(*tags)
      logger.tagged(*tags) { yield(self) }
    end

    private

    attr_reader :logger
  end
end
