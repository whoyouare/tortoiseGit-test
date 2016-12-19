package com.unicom.bigData.openPlatform.common.hbaseResultMapping;

import org.apache.hadoop.hbase.util.Bytes;

public class LongBytesConverter implements BytesConverter {

	public Object convert(byte[] bytes, Class clazz) {
		return Bytes.toLong(bytes);
	}

	public boolean canProcess(Class clazz) {
		return clazz.equals(Long.class) || clazz.equals(long.class);
	}
}
