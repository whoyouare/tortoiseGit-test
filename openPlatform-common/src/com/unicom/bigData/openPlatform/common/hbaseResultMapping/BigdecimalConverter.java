package com.unicom.bigData.openPlatform.common.hbaseResultMapping;

import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;

public class BigdecimalConverter implements BytesConverter {
	public Object convert(byte[] bytes, Class clazz) {
		String string = Bytes.toString(bytes);
		return string == null ? null : new BigDecimal(string);
	}

	public boolean canProcess(Class clazz) {
		return clazz.equals(BigDecimal.class);
	}
}
