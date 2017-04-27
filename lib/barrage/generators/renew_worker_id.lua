local max_value = tonumber(ARGV[1])
local new_worker_id = ARGV[2]
local old_worker_id = ARGV[3]
local ttl = tonumber(ARGV[4])
local race_condition_ttl = tonumber(ARGV[5])
local loop_cnt = 0

local worker_id = nil
local candidate_worker_id = tonumber(new_worker_id)

if type(old_worker_id) == "string" and string.len(old_worker_id) > 0 and redis.call('EXISTS', "barrage:worker:" .. old_worker_id) == 1 then
  redis.call("EXPIRE", "barrage:worker:" .. old_worker_id, ttl + race_condition_ttl)
  worker_id = old_worker_id
else
  while redis.call("SETNX", "barrage:worker:" .. candidate_worker_id, 1) == 0 and loop_cnt < max_value
  do
    candidate_worker_id = (candidate_worker_id + 1) % max_value
    loop_cnt = loop_cnt + 1
  end
  if loop_cnt >= max_value then
    return nil
  else
    worker_id = candidate_worker_id
  end
  redis.call("EXPIRE", "barrage:worker:" .. worker_id, ttl + race_condition_ttl)
end
return worker_id
