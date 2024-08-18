if redis.call("SETNX", KEYS[1], ARGV[1]) == 1 then
	redis.call("EXPIRE", KEYS[1], ARGV[2])
	return "OK"
else
	return 0
end
