require 'red_storm'

module RedStorm
  module Examples
    class WordCountBolt < RedStorm::SimpleBolt
      output_fields :word, :count
      on_init do
        @redis = Redis.new(:host => "localhost", :port => 6379, :db => 9)
        @counts = Hash.new{|h, k| h[k] = 0}
      end

      # block declaration style using auto-emit (default)
      #
      on_receive do |tuple|
        word = tuple.getValue(0).to_s
        @counts[word] += 1
        @redis.hincrby("results", word, 1)
        [word, @counts[word]]
      end

      on_close do
      end
    end
  end
end
