package com.unicom.bigData.openPlatform.common.cryption;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DigitalSignatureUtil {
	
//	static String ParamAccessKey = "accessKey";
//	
//	static String ParamVerifyToken = "verifyToken";
	
	String sharedKey = "!@#$%^&QWERTYLKJHGFcvbnm";
	
	String accessKey;
	String verifyToken;
	public String getSharedKey() {
		return sharedKey;
	}
	public void setSharedKey(String sharedKey) {
		this.sharedKey = sharedKey;
	}
	public String getAccessKey() {
		return accessKey;
	}
	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}
	public String getVerifyToken() {
		return verifyToken;
	}
	public void setVerifyToken(String verifyToken) {
		this.verifyToken = verifyToken;
	}
	
	
	public String createAccessKey() {
		long l = System.currentTimeMillis();
		return AesUtil.encrypt(l+"", sharedKey);
	}
	
	public String signKvs(Map<String, String> paraMap, String accessKey) {
		String pa = AesUtil.decrypt(accessKey, sharedKey);
		String md = digest(paraMap);		
		return MD5Util.MD5(md + pa);
	}
	
	public String digest(Map<String, String> paraMap) {
		String [] keys = new String[paraMap.size()];
		int cnt = 0;
		Iterator <String> iter = paraMap.keySet().iterator();
		
		while (iter.hasNext()) {
			keys[cnt ++] = iter.next();			
		}
		
		Arrays.sort(keys);
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < keys.length; i++) {
			sb.append(keys[i]+"="+paraMap.get(keys[i]));
		}
		
		return MD5Util.MD5(sb.toString());
	}
	
	public boolean verifySignature(String signature, String accessKey, Map<String, String> paraMap) {
		String nt = signKvs(paraMap, accessKey);	
		return signature.equals(nt);
	}
	
	public static void main(String[] args) {
		DigitalSignatureUtil signature = new DigitalSignatureUtil();
		
		//generate accessKey 
		String accessKey = signature.createAccessKey();
		Map<String, String> paraMap = new HashMap<String, String>();
		paraMap.put("name", "xlldees");
		paraMap.put("password", "1234");
		paraMap.put("acessKey", "UIOo332@");
		
		String token = signature.signKvs(paraMap, accessKey);
		
//		paraMap.put("acessKey", "UIOo332@1");
		boolean verify = signature.verifySignature(token, accessKey, paraMap);
		
		System.out.println(accessKey);
		System.out.println(token);
		System.out.println(verify);
	}

}
