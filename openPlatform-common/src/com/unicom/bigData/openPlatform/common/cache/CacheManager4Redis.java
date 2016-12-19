package com.unicom.bigData.openPlatform.common.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.unicom.bigData.openPlatform.common.OpenPlatformEnum;
import com.unicom.bigData.openPlatform.common.RedisHelperUtil;
import com.unicom.bigData.openPlatform.common.OpenPlatformEnum.RedisPositonKeys;

import redis.clients.jedis.ShardedJedis;

public class CacheManager4Redis implements ICacheManager {

	@Override
	public void put(String key, ICacheObject obj) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos;
		ShardedJedis jedis = null;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(obj);
			String bo = baos.toString();
			// put into redis
			jedis = RedisHelperUtil.getShardedJedis();
			jedis.set(OpenPlatformEnum.RedisGlobalKeys.secretTable.getName(), bo);
			oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			RedisHelperUtil.returnJedis(jedis);
		}

	}

	@Override
	public ICacheObject get(String key) {
		String s = null;// get from redis;
		ShardedJedis jedis = null;
		try {
			jedis = RedisHelperUtil.getShardedJedis();
			s = jedis.get(OpenPlatformEnum.RedisGlobalKeys.secretTable.getName());
			ByteArrayInputStream ins = new ByteArrayInputStream(s.getBytes());
			ObjectInputStream is;
			is = new ObjectInputStream(ins);
			Object obj = is.readUnshared();
			is.close();
			return (ICacheObject) obj;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			RedisHelperUtil.returnJedis(jedis);
		}

		return null;
	}

	public static void main(String[] args) {
		CacheManager4Redis cacheManager4Redis = new CacheManager4Redis();
		// cacheManager4Redis.get(key);

	}

}
