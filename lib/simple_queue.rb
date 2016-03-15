lib = File.expand_path('..', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

require 'rubygems'
require 'bundler/setup'
require 'active_support/tagged_logging'
require 'active_support/inflector'
require 'concurrent'

require 'simple_queue/logger'
require 'simple_queue/queue'
require 'simple_queue/consumer'
require 'simple_queue/executor'
require 'simple_queue/launcher'

module SimpleQueue
end