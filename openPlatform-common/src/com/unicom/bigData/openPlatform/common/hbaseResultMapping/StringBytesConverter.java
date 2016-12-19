package com.unicom.bigData.openPlatform.common.hbaseResultMapping;

import org.apache.hadoop.hbase.util.Bytes;

public class StringBytesConverter implements BytesConverter {

	public Object convert(byte[] bytes, Class clazz) {
		return Bytes.toString(bytes);
	}

	public boolean canProcess(Class clazz) {
		return clazz.equals(String.class);
	}
}
