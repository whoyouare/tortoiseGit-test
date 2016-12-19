package com.unicom.bigData.openPlatform.common.hbaseResultMapping;

import org.apache.hadoop.hbase.util.Bytes;

public class IntBytesConverter implements BytesConverter {
	public Object convert(byte[] bytes, Class clazz) {
		Integer integer = Bytes.toInt(bytes);
		return integer;
	}

	public boolean canProcess(Class clazz) {
		return clazz.equals(Integer.class) || clazz.equals(int.class);
	}
}
