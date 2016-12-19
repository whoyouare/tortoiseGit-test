package com.unicom.bigData.openPlatform.common.hbaseResultMapping;

public interface BytesConverter {
	
	Object convert(byte[] bytes, Class clazz);

	boolean canProcess(Class clazz);
}
