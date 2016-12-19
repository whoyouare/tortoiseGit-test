package com.unicom.bigData.openPlatform.common.cache;

public interface ICacheManager {
	
	public void put(String key, ICacheObject obj);
	
	public ICacheObject get(String key);

}
