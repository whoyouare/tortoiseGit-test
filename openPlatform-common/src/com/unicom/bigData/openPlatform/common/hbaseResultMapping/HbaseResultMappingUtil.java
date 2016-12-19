package com.unicom.bigData.openPlatform.common.hbaseResultMapping;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseResultMappingUtil {

	public static Object convertResultToObject(Result result, Class clazz) throws Exception {
		return convertResultToObject(result, clazz, null);
	}

	/**
	 * 
	 * @desc 方法描述
	 * @param 参数描述
	 * @return 返回值描述
	 * @throws 异常描述
	 * @date: 2016-11-8
	 */
	public static Object convertResultToObject(Result result, Class clazz, Object clazzObj) throws Exception {
		Map tempMap = null;
		if (clazzObj == null) {
			if (Map.class.isAssignableFrom(clazz) && Map.class.equals(clazz)) {
				clazzObj = new HashMap();
			} else {
				clazzObj = ConstructorUtils.invokeConstructor(clazz, null);
			}
		}
		if (Map.class.isAssignableFrom(clazz)) {
			tempMap = (Map) clazzObj;
		}

		if (result.getRow() == null) {
			return null;
		}
		List<Cell> cells = result.listCells();
		if (cells == null) {
			return null;
		}
		// 循环每一个result qualifies
		for (int i = 0; i < cells.size(); i++) {
			Cell c = cells.get(i);
			String qualify = Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
			byte[] value = c.getValue();

			if (Map.class.isAssignableFrom(clazz)) {
				tempMap.put(qualify, Bytes.toString(c.getValueArray(), c.getValueOffset(), c.getValueLength()));
			} else {
				Field mapField = FieldUtils.getDeclaredField(clazz, qualify, true);
				if (mapField == null) {
					continue;
				}
				// TODO：需要有个公共的配置去确定每一个表的字段的类型，目前根据根据clazz的属性的类型去判断
				FieldUtils.writeField(mapField, clazzObj,
						BytesConverterFactory.convertBytes(value, mapField.getType()), true);
			}
		}
		return clazzObj;
	}

}
