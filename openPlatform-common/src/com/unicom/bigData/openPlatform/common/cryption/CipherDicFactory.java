package com.unicom.bigData.openPlatform.common.cryption;

public class CipherDicFactory {
	
	public CipherDic createCipherDic() {
		return new AESCipherDic();
	}

}
