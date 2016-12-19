package com.unicom.bigData.openPlatform.common.hbaseResultMapping;

public class BytesConverterFactory {

	private static BytesConverter[] bytesConveterArray = new BytesConverter[] { new IntBytesConverter(),
			new StringBytesConverter(), new LongBytesConverter() };

	public static Object convertBytes(byte[] bytes, Class clazz) {
		for (BytesConverter bytesConverter : bytesConveterArray) {
			if (bytesConverter.canProcess(clazz)) {
				return bytesConverter.convert(bytes, clazz);
			}
		}
		throw new RuntimeException(clazz.getName() + "未定义转换器！");
	}
}
