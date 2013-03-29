require 'red_storm'
require 'redis'

module RedStorm
  module Examples
    class AggregateUsersBolt < RedStorm::SimpleBolt
      output_fields :name, :count

      on_init do
        @redis = Redis.new(:host => "localhost", :port => 6379, :db => 9)
        @counts = Hash.new{|h, k| h[k] = 0}
      end

      # block declaration style using auto-emit (default)
      #
      on_receive do |tuple|
        name = tuple.getValue(0).to_s
        @counts[name] += 1
        @redis.hincrby("results", name, 1)

        [name, @counts[name]]
      end
    end
  end
end
