package com.unicom.bigData.openPlatform.common.cryption;

import com.unicom.bigData.openPlatform.common.cache.CacheManagerFactory;
import com.unicom.bigData.openPlatform.common.cache.ICacheManager;

public class PhoneNumEncryptor {
	
	private CipherDic pubCipherDic;
	String key = "ASDFhjkl!@#$%67890"; 
//	private Map<String, CipherDic> privateCipherDicMap = new HashMap<String, CipherDic>();
	ICacheManager cacheManager = CacheManagerFactory.getInstance().createCacheManager();
	
	CipherDicFactory factory = new CipherDicFactory();
	
	public CipherDic loadCipherDics(String privateKey) {
		if (pubCipherDic == null) {
			synchronized (this) {
				if (pubCipherDic == null) {
					pubCipherDic = factory.createCipherDic();
					pubCipherDic.setKey(key);
					pubCipherDic.init();
					//"pubCipherDic" pubCipherDic
				}
			}
		}
		
		CipherDic priCipherDic = (CipherDic) cacheManager.get(privateKey);
		if (priCipherDic == null) {
			synchronized (this) {
				priCipherDic = (CipherDic) cacheManager.get(privateKey);
				if (priCipherDic == null) {
					priCipherDic = factory.createCipherDic();
					priCipherDic.setKey(privateKey);
					priCipherDic.init();
					cacheManager.put(privateKey, priCipherDic);
					//"privateCipherDicMap" privateCipherDicMap
				}
			}
		}
		return priCipherDic;
	}
	
	public String encrypt(String phoneNum, String privateKey) {
		CipherDic priCipherDic = loadCipherDics(privateKey); 
		
		String oriN = phoneNum.substring(0, 3);
		String priN = phoneNum.substring(3, 7);
		String pubN = phoneNum.substring(7, 11);
		
		String newN = oriN + priCipherDic.getCipherNum(Integer.parseInt(priN)) +
				pubCipherDic.getCipherNum(Integer.parseInt(pubN));
		
		return newN;
	}
	
	public String decrypt(String phoneNum, String privateKey) {
		CipherDic priCipherDic = loadCipherDics(privateKey); 
		
		String oriN = phoneNum.substring(0, 3);
		String priN = phoneNum.substring(3, 7);
		String pubN = phoneNum.substring(7, 11);
		
		String newN = oriN + priCipherDic.getClearTxtNum(Integer.parseInt(priN)) +
				pubCipherDic.getClearTxtNum(Integer.parseInt(pubN));
		
		return newN;
	}
	
	public static void main(String[] args) {
		PhoneNumEncryptor phoneNumEncryptor = new PhoneNumEncryptor();
		String phone = "13845678901";
		String privateKey = "923jjjsd";//WERTYUI^&*()
		String en = phoneNumEncryptor.encrypt(phone, privateKey);
		String de = phoneNumEncryptor.decrypt(phone, privateKey);
		System.out.println(phone);
		System.out.println(en);
		System.out.println(de);
	}

}
