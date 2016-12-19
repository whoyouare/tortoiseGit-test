package com.unicom.bigData.openPlatform.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.json.JSONException;
import org.json.JSONObject;

import com.unicom.bigData.openPlatform.common.hbaseResultMapping.HbaseResultMappingUtil;

/**
 * 
 * @name HBaseHelperUtlis.java
 * @author gary.qin
 * @version 2016-10-17
 */
public class HBaseHelperUtlis {
	private static Configuration conf = HBaseConfiguration.create();;
	private static byte[] fm = Bytes.toBytes("info");

	public static List getObjectsByPrefixFilter(String tableName,
			String rowPrefix, Class clazz) throws Exception {
		Filter filter = new PrefixFilter(rowPrefix.getBytes());
		return getObjectsByFilter(tableName, null, filter, clazz);
	}

	public static List getObjectsByFilter(String tableName, Integer limit,
			Filter filter, Class clazz) throws Exception {
		// IqiyiHBaseHeplerUtils.initKerberosVerifyIfNeed();
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();
		HTable htable = new HTable(conf, tableName);
		System.out.println("----------init htable success------!");
		Scan scan = new Scan();
		if (filter != null)
			scan.setFilter(filter);
		if (limit != null) {
			scan.setCaching(limit);
		}
		ResultScanner rs = htable.getScanner(scan);
		int index = 0;
		if (rs != null) {
			List returnObjs = new ArrayList();
			for (Result r : rs) {
				if (clazz == null) {
					returnObjs.add(r);
				} else {
					returnObjs.add(HbaseResultMappingUtil
							.convertResultToObject(r, clazz));

				}
				index++;
				if (index + 1 > limit)
					break;
			}
			return returnObjs;
		}
		htable.close();
		return null;
	}

	// get row by one row key
	public static Object getObject(String tableName, byte[] row, Class clazz)
			throws Exception {
		Get get = new Get(row);
		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();
		// HTable htable = new HTable(conf,tableName);
		HTable htable = new HTable(conf, tableName);
		Result result = htable.get(get);
		htable.close();
		// printResult(result);
		if (clazz == null) {
			return result;
		}
		return HbaseResultMappingUtil.convertResultToObject(result, clazz);
	}

	public static List getMutiStringRowObjects(String tableName, Class clazz,
			List<String> rows) throws Exception {

		if (rows != null && rows.size() > 0) {
			List<byte[]> bs = new ArrayList<byte[]>();
			for (String row : rows) {
				bs.add(Bytes.toBytes(row));
			}
			return getMutiRowObjects(tableName, clazz, bs);
		}
		return null;
	}

	public static List getMutiRowObjects(String tableName, Class clazz,
			List<byte[]> rows) throws Exception {

		return getMutiRowObject(tableName, clazz,
				rows.toArray(new byte[rows.size()][]));

	}

	// get muti rows by row keys
	public static List getMutiRowObject(String tableName, Class clazz,
			byte[]... rows) throws Exception {

		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();
		if (rows != null && rows.length > 0) {
			List<Get> queryRowList = new ArrayList<Get>();
			for (byte[] row : rows) {
				queryRowList.add(new Get(row));
			}
			HTable htable = new HTable(conf, tableName);
			Result[] results = htable.get(queryRowList);
			if (results != null && results.length > 0) {
				List returnObjs = new ArrayList();
				for (Result r : results) {
					if (clazz == null) {
						returnObjs.add(r);
					} else {
						returnObjs.add(HbaseResultMappingUtil
								.convertResultToObject(r, clazz));

					}
				}
				return returnObjs;
			}
		}
		return null;
	}

	public static void printResult(String tableName, byte[] row)
			throws IOException {
		Get get = new Get(row);
		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();
		// HTable htable = new HTable(conf,tableName);
		HTable htable = new HTable(conf, tableName);
		Result result = htable.get(get);
		htable.close();
		printResult(result);
	}

	public static void put(String tableName, String rowKey, String key,
			String value) throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();
		HTable htable = new HTable(conf, tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(fm, Bytes.toBytes(key), Bytes.toBytes(value));
		htable.put(put);
		htable.close();
	}

	public static void putList(String tableName, List<Put> puts)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();
		HTable htable = new HTable(conf, tableName);
		htable.put(puts);
		htable.close();
	}

	public static Put resultToPut(Result r) throws IOException {
		Put put = new Put(r.getRow());
		for (Cell c : r.listCells())
			put.add(c);
		return put;
	}

	public static void printResult(String tableName, List<byte[]> rows)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();
		HTable htable = new HTable(conf, tableName);
		List<Get> gets = new ArrayList<Get>();
		for (byte[] row : rows) {
			Get get = new Get(row);
			gets.add(get);
		}
		Result[] results = htable.get(gets);
		htable.close();
		for (Result r : results)
			printResult(r);
	}

	/**
	 * 打印HBase表在hdfs上存储大小
	 * 
	 * @param tableName
	 *            hbase表名
	 * @throws IOException
	 */
	public static void printHTableSize(String tableName) throws IOException {
		long size = TranswarpHBaseHeplerUtils.getHTableSize(tableName);
		long gunit = size / (1024 * 1024 * 1024);
		System.out.println(tableName + " " + size + " " + gunit + "G");
	}

	/**
	 * 以字符串方式打印出Put对象
	 * 
	 * @param put
	 */
	public static void printPut(Put put) {
		String row = Bytes.toString(put.getRow());
		StringBuffer sb = new StringBuffer();
		sb.append("row:").append(row).append(" ");
		for (Entry<byte[], List<Cell>> entry : put.getFamilyCellMap()
				.entrySet()) {
			for (Cell cell : entry.getValue()) {
				String q = Bytes.toString(cell.getQualifierArray(),
						cell.getQualifierOffset(), cell.getQualifierLength());
				String v = Bytes.toString(cell.getValueArray(),
						cell.getValueOffset(), cell.getValueLength());
				sb.append(q).append(":").append(v).append(" ");
			}
		}
		System.out.println(sb.toString());
	}

	/**
	 * 以字符串方式打印出hbase Result对象
	 * 
	 * @param r
	 */
	public static void printResult(Result r) {

		if (r == null) {
			System.out.println("---------result is null----");
			return;
		}
		String row = Bytes.toString(r.getRow());
		List<Cell> cells = r.listCells();
		if (cells == null) {
			System.out.println("---------result is null----");
			return;
		}
		StringBuffer sb = new StringBuffer();
		sb.append("row:").append(row).append(" \n");
		for (int i = 0; i < cells.size(); i++) {
			Cell c = cells.get(i);
			String qualify = Bytes.toString(c.getQualifierArray(),
					c.getQualifierOffset(), c.getQualifierLength());
			String value = Bytes.toString(c.getValueArray(),
					c.getValueOffset(), c.getValueLength());
			sb.append(qualify + ":").append(value).append("\n");
		}
		System.out.println(sb.toString());
	}

	/**
	 * 以字符串方式打印出hbase Result对象
	 * 
	 * @param r
	 */
	public static void printResultOneLine(Result r) {

		if (r == null) {
			System.out.println("---------result is null----");
			return;
		}
		String row = Bytes.toString(r.getRow());
		List<Cell> cells = r.listCells();
		if (cells == null) {
			System.out.println("---------result is null----");
			return;
		}
		StringBuffer sb = new StringBuffer();
		sb.append("row:").append(row).append(" \n");
		for (int i = 0; i < cells.size(); i++) {
			Cell c = cells.get(i);
			String qualify = Bytes.toString(c.getQualifierArray(),
					c.getQualifierOffset(), c.getQualifierLength());
			String value = Bytes.toString(c.getValueArray(),
					c.getValueOffset(), c.getValueLength());
			sb.append(qualify + ":").append(value).append("");
		}
		System.out.println(sb.toString());
	}

	/**
	 * 以字符串方式打印出hbase Result对象
	 * 
	 * @param r
	 */
	public static void printTblResult(Result r, String tableName) {

		if (r == null) {
			System.out.println("---------result is null----");
			return;
		}
		String row = Bytes.toString(r.getRow());
		List<Cell> cells = r.listCells();
		if (cells == null) {
			System.out.println("---------result is null----");
			return;
		}
		StringBuffer sb = new StringBuffer();
		sb.append("row:").append(row).append(" \n");
		long maxTimeStamp = 0;
		for (int i = 0; i < cells.size(); i++) {
			Cell c = cells.get(i);
			String qualify = Bytes.toString(c.getQualifierArray(),
					c.getQualifierOffset(), c.getQualifierLength());
			String value = null;
			if ("bzsys_total_comments".equals(tableName)
					&& (qualify.equals("aRe") || "aWA".equals(qualify))) {
				value = Bytes.toBoolean(c.getValue()) + "";
			} else if ("bzsys_total_comments".equals(tableName)
					&& (qualify.equals("aSen"))) {
				value = Bytes.toInt(c.getValueArray(), c.getValueOffset(),
						c.getValueLength())
						+ "";
			} else
				value = Bytes.toString(c.getValueArray(), c.getValueOffset(),
						c.getValueLength());
			sb.append(qualify + ":").append(value).append("\n");
			long timeStamp = c.getTimestamp();
			maxTimeStamp = timeStamp > maxTimeStamp ? timeStamp : maxTimeStamp;
		}
		System.out.println(sb.toString());
		System.out.println();
	}

	public static void truncateHTable(String tableName) {
		try {
			deleteHTable(tableName);
			createTable(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 以字符串方式打印出hbase Result对象
	 * 
	 * @param r
	 */
	public static String printResult(Result r, String name) {
		byte[] value = r.getValue(Bytes.toBytes("info"), Bytes.toBytes(name));
		if (value != null) {
			System.out.println(Bytes.toString(value));
			return Bytes.toString(value);
		}
		return null;
	}

	/**
	 * 查看HBase表数据格式(row和键值对)
	 * 
	 * @param tableName
	 *            表名
	 * @param limit
	 *            查看条数
	 * @throws IOException
	 */
	public static void lookHTableData(String tableName, int limit)
			throws IOException {
		lookHTableDataByFilter(tableName, limit, null);
	}

	public static void lookHTableDataToJSON(String tableName, int limit) {
		try {
			lookHTableDataByFilterToJSON(tableName, limit, new PageFilter(
					limit <= 0 ? 1 : limit));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static List<String> queryHTableDataToJSON(String tableName, int limit) {
		try {
			return queryHTableDataByFilterToJSON(tableName, limit,
					new PageFilter(limit <= 0 ? 1 : limit));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 查看HBase表数据格式(row和键值对)
	 * 
	 * @param tableName
	 *            表名
	 * @param limit
	 *            查看条数
	 * @param Filter
	 *            过滤器
	 * @throws IOException
	 */
	public static List<String> queryHTableDataByFilterToJSON(String tableName,
			int limit, Filter filter) throws IOException {
		// IqiyiHBaseHeplerUtils.initKerberosVerifyIfNeed();
		List<String> list = new ArrayList<String>();
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();
		HTable htable = new HTable(conf, tableName);
		System.out.println("----------init htable success------!");
		Scan scan = new Scan();
		if (filter != null)
			scan.setFilter(filter);
		scan.setCaching(limit);
		ResultScanner rs = htable.getScanner(scan);
		for (Result r : rs) {
			String json = parseResultToJSON(r);
			System.out.println(json);
			list.add(json);
		}
		htable.close();
		return list;
	}

	/**
	 * 查看HBase表数据格式(row和键值对)
	 * 
	 * @param tableName
	 *            表名
	 * @param limit
	 *            查看条数
	 * @param Filter
	 *            过滤器
	 * @throws IOException
	 */
	public static void lookHTableDataByFilterToJSON(String tableName,
			int limit, Filter filter) throws IOException {
		// IqiyiHBaseHeplerUtils.initKerberosVerifyIfNeed();
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();
		HTable htable = new HTable(conf, tableName);
		System.out.println("----------init htable success------!");
		Scan scan = new Scan();
		if (filter != null)
			scan.setFilter(filter);
		scan.setCaching(limit);
		ResultScanner rs = htable.getScanner(scan);
		for (Result r : rs) {
			System.out.println(parseResultToJSON(r));
		}

		htable.close();
	}

	public static String parseResultToJSON(Result r) {
		JSONObject obj = new JSONObject();
		if (r == null) {
			System.out.println("---------result is null----");
			return obj.toString();
		}
		String row = Bytes.toString(r.getRow());
		List<Cell> cells = r.listCells();
		if (cells == null) {
			System.out.println("---------result is null----");
			return obj.toString();
		}
		long maxTimeStamp = 0;
		for (int i = 0; i < cells.size(); i++) {
			Cell c = cells.get(i);
			String qualify = Bytes.toString(c.getQualifierArray(),
					c.getQualifierOffset(), c.getQualifierLength());
			String value = Bytes.toString(c.getValueArray(),
					c.getValueOffset(), c.getValueLength());
			long timeStamp = c.getTimestamp();
			maxTimeStamp = timeStamp > maxTimeStamp ? timeStamp : maxTimeStamp;
			try {
				obj.put(qualify, value);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		// try {
		// obj.put("timestamp",DateHelpUtils.getTimeAsString(maxTimeStamp));
		// } catch (JSONException e) {
		// e.printStackTrace();
		// }
		return obj.toString();
	}

	/**
	 * 查看HBase表数据格式(row和键值对)
	 * 
	 * @param tableName
	 *            表名
	 * @param limit
	 *            查看条数
	 * @param Filter
	 *            过滤器
	 * @throws IOException
	 */
	public static void lookHTableDataByFilter(String tableName, int limit,
			Filter filter) throws IOException {
		// IqiyiHBaseHeplerUtils.initKerberosVerifyIfNeed();
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();
		HTable htable = new HTable(conf, tableName);
		System.out.println("----------init htable success------!");
		Scan scan = new Scan();
		if (filter != null)
			scan.setFilter(filter);
		scan.setCaching(limit);
		ResultScanner rs = htable.getScanner(scan);
		int index = 0;
		for (Result r : rs) {
			printTblResult(r, tableName);
			index++;
			if (index + 1 > limit)
				break;
		}

		htable.close();
	}

	/**
	 * 查看HBase表数据格式(row和键值对)
	 * 
	 * @param tableName
	 *            表名
	 * @param limit
	 *            查看条数
	 * @param Filter
	 *            过滤器
	 * @throws IOException
	 */
	public static void lookHTableDataOnlyKey(String tableName, int limit)
			throws IOException {
		// IqiyiHBaseHeplerUtils.initKerberosVerifyIfNeed();
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();
		HTable htable = new HTable(conf, tableName);
		System.out.println("----------init htable success------!");
		Scan scan = new Scan();
		scan.setCaching(limit);
		ResultScanner rs = htable.getScanner(scan);
		int index = 0;
		for (Result r : rs) {
			if (r == null) {
				System.out.println("---------result is null----");
				return;
			}
			String row = Bytes.toString(r.getRow());
			List<Cell> cells = r.listCells();
			if (cells == null) {
				System.out.println("---------result is null----");
				return;
			}
			StringBuffer sb = new StringBuffer();
			sb.append("row:").append(row).append(" \n");
			for (int i = 0; i < cells.size(); i++) {
				Cell c = cells.get(i);
				String qualify = Bytes.toString(c.getQualifierArray(),
						c.getQualifierOffset(), c.getQualifierLength());

				sb.append(qualify + ":").append("\n");
			}
			System.out.println(sb.toString());
			index++;
			if (index + 1 > limit)
				break;
		}

		htable.close();
	}

	public static List<Result> getLimitResult(String tableName, int limit)
			throws IOException {
		// IqiyiHBaseHeplerUtils.initKerberosVerifyIfNeed();
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();
		HTable htable = new HTable(conf, tableName);
		System.out.println("----------init htable success------!");
		Scan scan = new Scan();
		scan.setCaching(limit);
		List<Result> list = new ArrayList<Result>();
		ResultScanner rs = htable.getScanner(scan);
		int index = 0;
		for (Result r : rs) {
			list.add(r);
			index++;
			if (index + 1 > limit)
				break;
		}

		htable.close();
		return list;
	}

	/**
	 * 查看HBase表数据格式(row和键值对)
	 * 
	 * @param tableName
	 *            表名
	 * @param limit
	 *            查看条数
	 * @param filters
	 *            过滤器
	 * @throws IOException
	 */
	public static void lookHTableData(String tableName, int limit, Filter filter)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();
		HTable htable = new HTable(conf, tableName);
		System.out.println("----------init htable success------!");
		Scan scan = new Scan();
		if (filter != null)
			scan.setFilter(filter);
		scan.setCaching(limit);
		// scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("aid"));
		ResultScanner rs = htable.getScanner(scan);
		int index = 0;
		for (Result r : rs) {
			printResult(r);
			index++;
			if (index + 1 > limit)
				break;
		}

		htable.close();
	}

	/**
	 * 查看HBase表数据格式(row和键值对)
	 * 
	 * @param tableName
	 *            表名
	 * @param limit
	 *            查看条数
	 * @param filters
	 *            过滤器
	 * @throws IOException
	 */
	public static void lookHTableData(String tableName, Scan scan)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();
		HTable htable = new HTable(conf, tableName);
		System.out.println("----------init htable success------!");
		// scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("aid"));
		ResultScanner rs = htable.getScanner(scan);
		int index = 0;
		for (Result r : rs) {
			printResult(r);
		}

		htable.close();
	}

	public static List lookHtableData(String tableName, Class class1)
			throws Exception {
		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();
		HTable htable = new HTable(conf, tableName);
		Scan scan = new Scan();
		ResultScanner rs = htable.getScanner(scan);
		List resultList = new ArrayList();
		for (Result r : rs) {
			resultList.add(HbaseResultMappingUtil.convertResultToObject(r,
					class1));

		}
		return resultList;
	}

	/**
	 * 查看HBase表数据格式(row和键值对)
	 * 
	 * @param tableName
	 *            表名
	 * @param limit
	 *            查看条数
	 * @throws IOException
	 */
	public static void lookHTableData(String tableName, String column, int limit)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();
		HTable htable = new HTable(conf, tableName);
		System.out.println("----------init htable success------!");
		Scan scan = new Scan();
		scan.setCaching(limit);
		ResultScanner rs = htable.getScanner(scan);
		int index = 0;
		for (Result r : rs) {
			printResult(r, column);
			index++;
			if (index + 1 > limit)
				break;
		}

		htable.close();
	}

	public static void testLookHTableData(String tableName, int limit)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();
		HTable htable = new HTable(conf, tableName);
		System.out.println("----------init htable success------!");
		Scan scan = new Scan();
		scan.setCaching(limit);
		ResultScanner rs = htable.getScanner(scan);
		int index = 0;
		for (Result r : rs) {
			printResult(r);
			index++;
			if (index + 1 >= limit)
				break;
		}

		htable.close();
	}

	public static void testPrintResult(Result r) {
		String row = Bytes.toString(r.getRow());
		List<Cell> cells = r.listCells();
		StringBuffer sb = new StringBuffer();
		sb.append("row:").append(row).append(" \n");
		for (int i = 0; i < cells.size(); i++) {
			Cell c = cells.get(i);
			String qualify = Bytes.toString(c.getQualifierArray(),
					c.getQualifierOffset(), c.getQualifierLength());
			String value = Bytes.toString(c.getValueArray(),
					c.getValueOffset(), c.getValueLength());
			sb.append(qualify + ":").append(value).append("\n");
		}
		System.out.println(sb.toString());
	}

	public static void deleteHTable(String tableName) throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		admin.close();
	}

	// /
	public static void deleteData(String tableName) throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();
		HTable htable = new HTable(conf, tableName);
		ResultScanner rs = htable.getScanner(new Scan());
		List<Delete> deletes = new ArrayList<Delete>();
		for (Result r : rs) {
			deletes.add(new Delete(r.getRow()));
		}
		htable.delete(deletes);
		htable.close();
	}

	public static void deleteData(String tableName, byte[] row)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();
		HTable htable = new HTable(conf, tableName);
		htable.delete(new Delete(row));
		htable.close();
	}
	
	public static void deleteData(String tableName, List<byte[]> rows)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();
		HTable htable = new HTable(conf, tableName);
		List<Delete> deletes = new ArrayList<Delete>();
		for (byte[] r : rows) {
			deletes.add(new Delete(r));
		}
		htable.delete(deletes);
		htable.close();
	}

	public static void createPreHRegionHTable(String tableName)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor desc = new HTableDescriptor(tableName);
		HColumnDescriptor hcd = new HColumnDescriptor("info");
		desc.addFamily(new HColumnDescriptor("info"));
		// admin.createTableAsync(arg0, arg1);
		admin.createTable(desc);
		admin.close();
	}

	public static void createTable(String tableName) throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor desc = new HTableDescriptor(tableName);
		HColumnDescriptor hcd = new HColumnDescriptor("info");
		hcd.setCompressionType(Algorithm.SNAPPY);
		desc.addFamily(hcd);
		// admin.createTableAsync(arg0, arg1);
		admin.createTable(desc);
		admin.close();
	}

	public static void renameHTable(String oldTableName, String newTableName)
			throws IOException, InterruptedException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();

		HBaseAdmin admin = new HBaseAdmin(conf);
		String snapshotName = "bzsys_tmp_test_001";
		admin.enableTable(oldTableName);
		/*
		 * admin.disableTable(oldTableName); admin.snapshot(snapshotName,
		 * oldTableName); admin.cloneSnapshot(snapshotName, newTableName);
		 * admin.deleteSnapshot(snapshotName); admin.deleteTable(oldTableName);
		 */
		admin.close();
	}

	public static void createTable(String tableName, byte[][] splitKeys)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();

		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor desc = new HTableDescriptor(tableName);
		HColumnDescriptor cf = new HColumnDescriptor("info");
		cf.setCompressionType(Algorithm.SNAPPY);
		desc.addFamily(cf);
		// admin.createTableAsync(arg0, arg1);
		admin.createTable(desc, splitKeys);
		admin.close();
	}

	public static void createTable(String tableName, int maxVersion)
			throws IOException {
		if (maxVersion <= 0)
			maxVersion = 1;
		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();

		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor desc = new HTableDescriptor(tableName);
		HColumnDescriptor hcd = new HColumnDescriptor("info");
		hcd.setMaxVersions(maxVersion);
		desc.addFamily(hcd);
		// admin.createTableAsync(arg0, arg1);
		admin.createTable(desc);
		admin.close();
	}

	public static long printHTableHRegionSize(String tableName)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerify();

		HTable htable = new HTable(conf, tableName);
		System.out.println("------init HTable " + tableName
				+ "- connection success!------");
		RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(htable);
		long size = 0;
		Pair<byte[][], byte[][]> keys = htable.getStartEndKeys();
		List<Integer> list = new ArrayList<Integer>();
		List<Integer> regionDescList = new ArrayList<Integer>();
		long totalSize = 0;
		for (int i = 0; i < keys.getFirst().length; i++) {
			HRegionLocation location = htable.getRegionLocation(
					keys.getFirst()[i], false);
			byte[] regionName = location.getRegionInfo().getRegionName();
			long regionSize = sizeCalculator.getRegionSize(regionName);
			totalSize += regionSize;
			System.out.println(Bytes.toString(regionName) + ":" + regionSize
					/ (1024 * 1024) + "M");
			regionDescList.add((int) (regionSize / (1024 * 1024)));
			list.add((int) (regionSize / (1024 * 1024)));
		}
		htable.close();
		Collections.sort(regionDescList);
		for (int s : regionDescList)
			System.out.println(s);
		System.out.println("----total size---" + totalSize);
		return size;
	}

	public static void printHTableStartStopRow(String tableName)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();

		Scan scan = new Scan();
		HTable table = new HTable(conf, tableName);
		RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(table);
		Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
		if (keys == null || keys.getFirst() == null
				|| keys.getFirst().length == 0) {
			HRegionLocation regLoc = table.getRegionLocation(
					HConstants.EMPTY_BYTE_ARRAY, false);
			if (null == regLoc) {
				throw new IOException("Expecting at least one region.");
			}
			List<InputSplit> splits = new ArrayList<InputSplit>(1);
			long regionSize = sizeCalculator.getRegionSize(regLoc
					.getRegionInfo().getRegionName());
		}
		List<InputSplit> splits = new ArrayList<InputSplit>(
				keys.getFirst().length);
		for (int i = 0; i < keys.getFirst().length; i++) {
			HRegionLocation location = table.getRegionLocation(
					keys.getFirst()[i], false);
			byte[] startRow = scan.getStartRow();
			byte[] stopRow = scan.getStopRow();
			// determine if the given start an stop key fall into the region
			if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
					.compareTo(startRow, keys.getSecond()[i]) < 0)
					&& (stopRow.length == 0 || Bytes.compareTo(stopRow,
							keys.getFirst()[i]) > 0)) {
				byte[] splitStart = startRow.length == 0
						|| Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys
						.getFirst()[i] : startRow;
				byte[] splitStop = (stopRow.length == 0 || Bytes.compareTo(
						keys.getSecond()[i], stopRow) <= 0)
						&& keys.getSecond()[i].length > 0 ? keys.getSecond()[i]
						: stopRow;

				byte[] regionName = location.getRegionInfo().getRegionName();
				long regionSize = sizeCalculator.getRegionSize(regionName);
				// TableSplit split = new TableSplit(table.getName(),
				// splitStart, splitStop, null, regionSize);
				System.out.println("startRow:[" + Bytes.toString(splitStart)
						+ "] stopRow:[" + Bytes.toString(splitStop)
						+ "] hregionName:[" + Bytes.toString(regionName)
						+ "] hregionSize:[" + regionSize + "]");
			}
		}
	}

	public static List<String> getHTableStartStopRow(String tableName)
			throws IOException {
		List<String> list = new ArrayList<String>();
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();

		Scan scan = new Scan();
		HTable table = new HTable(conf, tableName);
		RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(table);
		Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
		if (keys == null || keys.getFirst() == null
				|| keys.getFirst().length == 0) {
			HRegionLocation regLoc = table.getRegionLocation(
					HConstants.EMPTY_BYTE_ARRAY, false);
			if (null == regLoc) {
				throw new IOException("Expecting at least one region.");
			}
			List<InputSplit> splits = new ArrayList<InputSplit>(1);
			long regionSize = sizeCalculator.getRegionSize(regLoc
					.getRegionInfo().getRegionName());
		}
		List<InputSplit> splits = new ArrayList<InputSplit>(
				keys.getFirst().length);
		for (int i = 0; i < keys.getFirst().length; i++) {
			HRegionLocation location = table.getRegionLocation(
					keys.getFirst()[i], false);
			byte[] startRow = scan.getStartRow();
			byte[] stopRow = scan.getStopRow();
			// determine if the given start an stop key fall into the region
			if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
					.compareTo(startRow, keys.getSecond()[i]) < 0)
					&& (stopRow.length == 0 || Bytes.compareTo(stopRow,
							keys.getFirst()[i]) > 0)) {
				byte[] splitStart = startRow.length == 0
						|| Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys
						.getFirst()[i] : startRow;
				byte[] splitStop = (stopRow.length == 0 || Bytes.compareTo(
						keys.getSecond()[i], stopRow) <= 0)
						&& keys.getSecond()[i].length > 0 ? keys.getSecond()[i]
						: stopRow;

				byte[] regionName = location.getRegionInfo().getRegionName();
				long regionSize = sizeCalculator.getRegionSize(regionName);
				// TableSplit split = new TableSplit(table.getName(),
				// splitStart, splitStop, null, regionSize);
				// System.out.println("startRow:["+Bytes.toString(splitStart)+"] stopRow:["+Bytes.toString(splitStop)+"] hregionName:["+Bytes.toString(regionName)+"] hregionSize:["+regionSize+"]");
				list.add(Bytes.toString(splitStart));
				list.add(Bytes.toString(splitStop));
			}
		}
		return list;
	}

	public static String printData(String tableName, byte[] row, String column)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerify();

		HTable htable = new HTable(conf, tableName);
		System.out.println("---begin query htable " + tableName);
		Get get = new Get(row);
		Result result = htable.get(get);
		htable.close();
		return printResult(result, column);
	}

	public static void printRowData(String tableName, byte[] row, Filter filter)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerify();

		HTable htable = new HTable(conf, tableName);
		System.out.println("---begin query htable " + tableName);
		Get get = new Get(row);
		if (filter != null)
			get.setFilter(filter);
		Result result = htable.get(get);
		htable.close();
		printResult(result);
	}

	public static void printGetData(String tableName, String row)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerify();

		HTable htable = new HTable(conf, tableName);
		Get get = new Get(Bytes.toBytes(row));
		Result result = htable.get(get);
		htable.close();
		printResult(result);
	}

	// public static void printPeronFsNum(List<String>rows) throws IOException{
	// TranswarpHBaseHeplerUtils.initKerberosVerify();
	//
	// HTable htable = new HTable(conf, BzsysHTableConfig.TOTAL_HTABLE);
	// byte[]q=Bytes.toBytes("fPerId");
	// List<Get>gets=new ArrayList<Get>();
	// for(String row:rows){
	// Get get=new Get(Bytes.toBytes(row));
	// get.addColumn(fm,q);
	// gets.add(get);
	// }
	//
	// Result[]results=htable.get(gets);
	// List<Get>personGets=new ArrayList<Get>();
	// Set<String>set=new HashSet<String>();
	// for(Result r:results){
	// set.add(Bytes.toString(r.getValue(fm, q)));
	// if("1623886424".equals(Bytes.toString(r.getValue(fm, q))))
	// printResult(r);
	// }
	// for(String perId:set){
	// Get get=new Get(Bytes.toBytes(perId));
	// personGets.add(get);
	// }
	// htable.close();
	// HTable htable2 = new HTable(conf, BzsysHTableConfig.PERSON_HTABLE);
	// Result[]results2=htable2.get(personGets);
	// for(Result r:results2)
	// printResult(r);
	// htable2.close();
	// }
	public static void printDataByColumns(String tableName, byte[] row,
			List<String> columns) throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerify();

		HTable htable = new HTable(conf, tableName);
		Get get = new Get(row);
		Result result = htable.get(get);
		htable.close();
		printResult(result);
	}

	public static void printDocData(String key) throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerify();

		HTable htable = new HTable(conf, "bzsys_doc");
		Scan scan = new Scan();
		scan.setCaching(1000);
		scan.setBatch(500);
		List<Pair<byte[], byte[]>> list = new ArrayList<Pair<byte[], byte[]>>();
		list.add(new Pair<byte[], byte[]>(Bytes.toBytes(key), Bytes.toBytes(1)));
		scan.setFilter(new FuzzyRowFilter(list));

		ResultScanner rs = htable.getScanner(scan);
		for (Result result : rs)
			printResult(result);
		htable.close();
	}

	public static List<String> getStartStopKeys(String tableName)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();

		HTable htable = new HTable(conf, tableName);
		List<String> list = new ArrayList<String>();
		List<String> servers = new ArrayList<String>();
		NavigableMap<HRegionInfo, ServerName> regions = htable
				.getRegionLocations();
		for (Entry<HRegionInfo, ServerName> entry : regions.entrySet()) {
			servers.add(entry.getValue().getServerName());
			System.out.println(entry.getKey().toString() + " serverName:"
					+ entry.getValue().getServerName());
			list.add(Bytes.toString(entry.getKey().getStartKey()));
			list.add(Bytes.toString(entry.getKey().getEndKey()));

		}
		Collections.sort(list);
		for (int i = 0; i < list.size(); i++) {
			// System.out.println(list.get(i));
		}
		System.out.println(list.size() + " " + servers.size());
		htable.close();
		return list;
	}

	public static void deleteAndCreateHTable(String tableName)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyIfNeed();

		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		admin.close();
		createTable(tableName);
	}

	public static void copyDataToHTable(String srcTable, String destTable,
			int limit) throws IOException {
		deleteAndCreateHTable(destTable);
		HConnection hcon = HConnectionManager.createConnection(conf);
		HTableInterface htable = hcon.getTable(srcTable);
		System.out.println("----------init htable success------!");
		Scan scan = new Scan();
		scan.setCaching(limit);
		ResultScanner rs = htable.getScanner(scan);
		int index = 0;
		List<Result> results = new ArrayList<Result>();
		for (Result r : rs) {
			printResult(r);
			results.add(r);
			index++;
			if (index + 1 >= limit)
				break;
		}
		rs.close();
		htable.close();
		List<Put> puts = new ArrayList<Put>();
		for (Result r : results) {
			Put put = new Put(r.getRow());
			for (Cell cell : r.listCells())
				put.add(cell);
			puts.add(put);
		}
		HTableInterface destHTable = hcon.getTable(destTable);
		destHTable.put(puts);
		destHTable.close();
	}

	public static List<String> printPreHregion(String tableName)
			throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();
		HTable htable = new HTable(conf, tableName);
		Pair<byte[][], byte[][]> pair = htable.getStartEndKeys();
		Set<String> set = new HashSet<String>();
		byte[][] startKeys = pair.getFirst();
		for (byte[] b : startKeys)
			set.add(Bytes.toString(b));
		for (byte[] b : pair.getSecond())
			set.add(Bytes.toString(b));
		List<String> list = new ArrayList<String>();
		for (String b : set)
			list.add(b);
		Collections.sort(list);
		htable.close();
		for (int i = 0; i < list.size(); i++) {
			System.out.println(list.get(i));
		}
		return list;
	}

	public static void printScan(String htableName, byte[] startKey,
			byte[] stopKey, int limit) throws IOException {
		limit = limit <= 0 ? 10 : limit;
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();
		HTable htable = new HTable(conf, htableName);
		Scan scan = new Scan(startKey, stopKey);
		scan.setFilter(new PageFilter(limit));
		ResultScanner rs = htable.getScanner(scan);
		int index = 0;
		for (Result r : rs) {
			printResult(r);
			index++;
			if (index + 1 > limit)
				break;
		}

		htable.close();
	}

	public static Set<String> listTables() throws IOException {
		TranswarpHBaseHeplerUtils.initKerberosVerifyInJar();
		HTable htable = new HTable(conf, "hbase:meta");
		Scan scan = new Scan();

		scan.setCaching(10000);
		scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("server"));
		ResultScanner rs = htable.getScanner(scan);
		Set<String> set = new TreeSet<String>();
		for (Result r : rs) {
			String row = Bytes.toString(r.getRow());
			String[] arr = row.split(",");
			String tableName = arr[0];
			String lower = tableName.toLowerCase();
			if (lower.startsWith("cpr") || lower.startsWith("bzsys"))
				set.add(tableName);
			// printResult(r);
		}
		for (String tableName : set) {
			System.out.println(tableName);
		}
		htable.close();
		return set;

	}

	public static void main(String[] args) throws Exception {
		// verifyHTable("bzsys_doc");
		// deleteHTable("bzsys_music");
		// createTable("bzsys_music");
		// renameHTable("bzsys_tmp_test", "bzsys_tmp_test_new");
		// lookHTableData("bzsys_news_info", 10);

		// printHTableSize("bzsys_total_comments");
		// 0000`100278`1000000000625956639
		// 0000`111813`14658617166
		// doc
		// 0000`1044233724`3663729662697238
	}
}
