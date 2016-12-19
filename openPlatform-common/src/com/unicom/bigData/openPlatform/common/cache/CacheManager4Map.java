package com.unicom.bigData.openPlatform.common.cache;

import java.util.HashMap;
import java.util.Map;

public class CacheManager4Map implements ICacheManager {
	
	private Map<String, ICacheObject> cache = new HashMap<String, ICacheObject>();

	@Override
	public void put(String key, ICacheObject obj) { 
		cache.put(key, obj);
	}

	@Override
	public ICacheObject get(String key) { 
		return cache.get(key);
	}

}
