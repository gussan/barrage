require 'redis'
require 'redis/scripting'
require 'barrage/generators/base'

class Barrage
  module Generators
    class RedisWorkerId < Base
      RACE_CONDITION_TTL = 30
      self.required_options += %w(ttl)

      def initialize(options = {})
        @worker_id = nil
        @worker_ttl = 0
        @real_ttl   = 0
        super
        @data = []
        @finalizer_proc = Finalizer.new(@data)
        ObjectSpace.define_finalizer(self, @finalizer_proc)
      end

      def generate
        now = Time.now.to_i
        if @worker_ttl - now <= 0
          @data[1] = @worker_id = renew_worker_id
          # check redis after half of real ttl
          @worker_ttl = now + ttl / 2
          @real_ttl   = now + ttl
          @data[2] = @real_ttl
        end
        @worker_id
      end
      alias_method :current, :generate

      def ttl
        options["ttl"]
      end

      def redis
        @redis ||= @data[0] = Redis.new(options["redis"] || {})
      end

      class Finalizer
        def initialize(data)
          @pid = $$
          @data = data
        end

        def call(*args)
          return if @pid != $$
          redis, worker_id, real_ttl = *@data

          if redis.is_a?(Redis) and redis.connected?
            redis.del("barrage:worker:#{worker_id}") if real_ttl > Time.now.to_i
            redis.client.disconnect
          end
        end
      end

      private

      def renew_worker_id
        if @real_ttl - Time.now.to_i - RACE_CONDITION_TTL <= 0
          @worker_id = nil
        end
        new_worker_id = script.run(:renew_worker_id, [], [2 ** length, rand(2 ** length), @worker_id, ttl, RACE_CONDITION_TTL])
        new_worker_id or raise StandardError, "Renew redis worker id failed"
        new_worker_id.to_i
      end

      def script
        @script ||= Redis::Scripting::Module.new(redis, __dir__)
      end
    end
  end
end
