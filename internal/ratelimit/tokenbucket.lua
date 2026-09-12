-- KEYS[1] = rl:{tenant}; ARGV = rate, burst, cost. Returns {allowed, retry_after_ms}.
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local v = redis.call('HMGET', KEYS[1], 'tokens', 'ts_us')
local tokens = tonumber(v[1]) or burst
local ts = tonumber(v[2]) or now
tokens = math.min(burst, tokens + rate * math.max(0, now - ts) / 1000000)
local allowed, retry = 0, 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry = math.ceil((cost - tokens) / rate * 1000)
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts_us', tostring(now))
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, retry}
