package com.unicom.bigData.openPlatform.common.cryption;

import com.unicom.bigData.openPlatform.common.cache.ICacheObject;


public interface CipherDic extends ICacheObject {
	
	public String getKey();

	public void setKey(String key);
	
	public void init();
	
	public String getCipherNum(int n4);
	
	public String getClearTxtNum(int n4);

}
