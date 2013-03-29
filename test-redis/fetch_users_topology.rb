require 'rubygems'
require 'red_storm'

require 'redis'
require 'thread'
require 'fetch_user_bolt'
require 'aggregate_users_bolt'
require 'send_to_hbase_bolt'

module RedStorm
  module Examples

    # RedisWordSpout reads the Redis queue "test" on localhost:6379
    # and emits each word items pop'ed from the queue.

    class UsersSpout < RedStorm::SimpleSpout
      output_fields :user_id

      on_send {@q.pop.to_s if @q.size > 0}

      on_init do
        @q = Queue.new
        @redis_reader = detach_redis_reader
      end

      private

      def detach_redis_reader
        Thread.new do
          Thread.current.abort_on_exception = true

          redis = Redis.new(:host => "localhost", :port => 6379, :db => 9)
          loop do
            if data = redis.blpop("author", 0)
              @q << data[1]
            end
          end
        end
      end
    end

    class FetchUsersTopology < RedStorm::SimpleTopology
      spout UsersSpout

      bolt FetchUserBolt, parallelism: 3 do
        source UsersSpout, fields: ["user_id"]
      end

      bolt SendToHbaseBolt, parallelism: 3 do
        source FetchUserBolt, fields: ["uid","name"]
      end

      configure do |env|
        debug true
        set "topology.worker.childopts", "-Djruby.compat.version=RUBY1_9"
        case env
        when :local
          max_task_parallelism 20
        when :cluster
          max_task_parallelism 5
          num_workers 20
          max_spout_pending(1000);
        end
      end
    end
  end
end
