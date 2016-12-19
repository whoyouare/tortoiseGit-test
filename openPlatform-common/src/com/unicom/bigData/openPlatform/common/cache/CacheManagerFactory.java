package com.unicom.bigData.openPlatform.common.cache;

public class CacheManagerFactory {

	static CacheManagerFactory instance = new CacheManagerFactory();

	public static CacheManagerFactory getInstance() {
		return instance;
	}

	public ICacheManager createCacheManager() {
		// return new CacheManager4Map();
		return new CacheManager4Redis();
	}

}
