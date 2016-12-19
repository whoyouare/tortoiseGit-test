package com.unicom.bigData.openPlatform.common;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.*;

public class RedisHelperUtil {

	private Integer maxActive;
	private Integer maxIdle;
	private Integer maxWait;
	private Boolean testOnborrow = true;

	private List<JedisShardInfo> shards = new ArrayList();
	private ShardedJedisPool jedisPool = null;
	private static RedisHelperUtil helperUtil = new RedisHelperUtil();

	private RedisHelperUtil() {
		maxActive = 100;
		maxIdle = 10;
		maxWait = 10000;

		shards.add(new JedisShardInfo("localhost", 6379));
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(maxActive);
		poolConfig.setMaxIdle(maxIdle);
		poolConfig.setMaxWaitMillis(maxWait);
		poolConfig.setTestOnBorrow(testOnborrow);
		jedisPool = new ShardedJedisPool(poolConfig, shards);
	}

	public RedisHelperUtil getInstance() {
		return helperUtil;
	}

	public static ShardedJedis getShardedJedis() {
		try {
			return helperUtil.jedisPool.getResource();
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	public static void returnJedis(ShardedJedis arg0) {
		if (arg0 != null) {
			arg0.close();
		}
	}

}
