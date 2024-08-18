if redis.call("GET", KEYS[1]) == ARGV[1] then
	redis.call("DEL", KEYS[1])
	return 1
else
	return 0
end
