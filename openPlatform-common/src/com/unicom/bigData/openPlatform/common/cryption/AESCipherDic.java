package com.unicom.bigData.openPlatform.common.cryption;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public class AESCipherDic implements CipherDic, Serializable  { 
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String key;
	
	int min = 0;
	int max = 9999;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
	
	SimpleCipher [] oriDic;
	SimpleCipher [] dic;
	public void init() {
		dic = new SimpleCipher[max + 1];
		oriDic = new SimpleCipher[max + 1];
		for (int i = min; i <= max; i++) {
			 try {
				String encryptResultStr = new String(AesUtil.encrypt2Bytes(i+"", key),"utf-8");
				dic[i] = new SimpleCipher(i, 0, encryptResultStr);
				oriDic[i] = dic[i];
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}  
		}
		Arrays.sort(dic);
		for (int i = 0; i < dic.length; i++) {
			dic[i].newN = i;
		}
	}
	
	class SimpleCipher implements Comparable<SimpleCipher>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		int ori;
		int newN;
		String cypherTxt; 
		
		public SimpleCipher(int ori, int newN, String cypherTxt) {
			super();
			this.ori = ori;
			this.newN = newN;
			this.cypherTxt = cypherTxt;
		}
		
		public int compareTo(SimpleCipher o) {
			return cypherTxt.compareTo(o.cypherTxt);
		} 
		
	}
	
	public String getCipherNum(int n4) { 
		return appendWith0(oriDic[n4].newN+"");
	}	
	
	public String getClearTxtNum(int n4) {
		return appendWith0(oriDic[n4].ori +"");
	}
	
	public String appendWith0(String a) {
		String s = "";
		for (int i = 0; i < 4 - a.length(); i++) {
			s = s + "0";
		}
		return s + a;
	}
	
	  

}
