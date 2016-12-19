package com.unicom.bigData.openPlatform.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;
import org.apache.hadoop.security.UserGroupInformation;

public class TranswarpHBaseHeplerUtils {

	public static void main(String[] args) {
		initKerberosVerifyInJar();
	}
	public static void initKerberosVerify() {

		System.setProperty("java.security.krb5.realm", "HADOOP.QIYI.COM");
		System.setProperty("java.security.krb5.kdc", "10.121.40.30");

		boolean isWindows = true;
		try {
			String os = System.getProperty("os.name").toLowerCase();

			System.out.println("------operate system name------" + os);
			if (os.indexOf("windows") == -1)
				isWindows = false;
			if (isWindows)
				//jy bzsys  sh bzsys_puyu
				UserGroupInformation.loginUserFromKeytab("bzsys_puyu@HADOOP.QIYI.COM", "D:\\bzsys_puyu.keytab");
			else {
				System.out.println("-------linux verify!----------");
				UserGroupInformation.loginUserFromKeytab("bzsys_puyu@HADOOP.QIYI.COM",
						"/home/bzsys_puyu/bzsys_puyu.keytab");
			}
			System.out.println("-----hbase kerberos virify seccess!-----");
		} catch (IOException e) {
			System.out.println("-----hbase kerberos virify fail!-----");
		}
	}
	public static void initKerberosVerifyInWindows(String userName, String kerberosPath) {

		System.setProperty("java.security.krb5.realm", "HADOOP.QIYI.COM");
		System.setProperty("java.security.krb5.kdc", "10.121.40.30");
		boolean isWindows = true;
		try {
			String os = System.getProperty("os.name").toLowerCase();

			System.out.println("------operate system name------" + os);
			if (os.indexOf("windows") == -1)
				isWindows = false;
			if (isWindows)
				UserGroupInformation.loginUserFromKeytab(userName+"@HADOOP.QIYI.COM", kerberosPath);
			else {
				System.out.println("-------linux verify!----------");
				UserGroupInformation.loginUserFromKeytab(userName+"@HADOOP.QIYI.COM",
						"/home/bzsys_puyu/bzsys_puyu.keytab");
			}
			System.out.println("-----hbase kerberos virify seccess!-----");
		} catch (IOException e) {
			System.out.println("-----hbase kerberos virify fail!-----");
		}
	}
	/**
	 * 
	 * @param path kerberos秘钥文件路径
	 */
	public static void iniKerberosVerify(String path){
		System.out.println("-----kerberos path----"+path);
		if(path==null){
			System.out.println("---------here verify");
			initKerberosVerifyInJar();
			return;
		}
		System.setProperty("java.security.krb5.realm", "HADOOP.QIYI.COM");
		System.setProperty("java.security.krb5.kdc", "10.121.40.30");
		try {
			UserGroupInformation.loginUserFromKeytab("bzsys_puyu@HADOOP.QIYI.COM",path);
			System.out.println("-----hbase kerberos virify seccess!-----");
		} catch (IOException e) {
			
			System.out.println("-----hbase kerberos virify fail!-----");
			e.printStackTrace();
		}
	}
	public static void initKerberosVerifyIfNeed() {
//		boolean isWindows = true;
//		String os = System.getProperty("os.name").toLowerCase();
//		System.out.println("------operate system name------" + os);
//		if (os.indexOf("windows") == -1) {
//			isWindows = false;
//			return;
//		}
//		System.setProperty("java.security.krb5.realm", "HADOOP.QIYI.COM");
//		System.setProperty("java.security.krb5.kdc", "10.121.40.30");
//		try {
//			if (isWindows)
//				UserGroupInformation.loginUserFromKeytab("bzsys_puyu@HADOOP.QIYI.COM", "D:\\bzsys_puyu.keytab");
//			else {
//				System.out.println("-------linux verify!----------");
//				UserGroupInformation.loginUserFromKeytab("bzsys_puyu@HADOOP.QIYI.COM",
//						"/home/bzsys_puyu/bzsys_puyu.keytab");
//			}
//			System.out.println("-----hbase kerberos virify seccess!-----");
//		} catch (IOException e) {
//			System.out.println("-----hbase kerberos virify fail!-----");
//			e.printStackTrace();
//		}
	}
	/**
	 *
	 * @param path kerberos秘钥文件路径
	 */
	public static void iniKerberosVerify(String path, String userName){
		 System.setProperty("java.security.krb5.realm", "HADOOP.QIYI.COM");
		 // 注意：下面的语句配置多台kdc，以冒号分隔，顺序需按机器所在机房设置，如在济阳机房，则将kdc04-jylt.qiyi.hadoop放在最前面！
		 System.setProperty("java.security.krb5.kdc", "kdc01-shjj.qiyi.hadoop:kdc02-shjj.qiyi.hadoop:kdc03-bjdxt.qiyi.hadoop:kdc04-jylt.qiyi.hadoop"); 
		try {
			System.out.println(userName + "@HADOOP.QIYI.COM");
			System.out.println("----kerbernes path-----"+path);
			UserGroupInformation.loginUserFromKeytab(userName+"@HADOOP.QIYI.COM",path);
			System.out.println("---kerberos verify success---!");
		} catch (IOException e) {
			System.out.println("---kerberos verify fail---!");
			e.printStackTrace();
		}
	}
	//		System.setProperty("java.security.krb5.realm", "HADOOP.QIYI.COM");
	//System.setProperty("java.security.krb5.kdc", "kdc04-jylt.qiyi.hadoop:kdc01-shjj.qiyi.hadoop:kdc02-shjj.qiyi.hadoop:kdc03-bjdxt.qiyi.hadoop");
	public static void initKerberosVerifyInJarKdc() {
	    System.setProperty("java.security.krb5.realm", "HADOOP.QIYI.COM");
	    System.setProperty("java.security.krb5.kdc", "kdc04-jylt.qiyi.hadoop:kdc01-shjj.qiyi.hadoop:kdc02-shjj.qiyi.hadoop:kdc03-bjdxt.qiyi.hadoop");
		String path = "bzsys_puyu.keytab";
		InputStream is = TranswarpHBaseHeplerUtils.class.getClassLoader().getResourceAsStream(path);
		File keytab = new File(path);
		System.out.println("--initKerberosVerifyInJar method kerberos path--"+keytab.getAbsolutePath());
		try {
			if(!keytab.exists()){
				OutputStream os = new FileOutputStream(keytab);
				int bytesRead = 0;
				byte[] buffer = new byte[1024];
				while ((bytesRead = is.read(buffer, 0, 1024)) != -1) {
					os.write(buffer, 0, bytesRead);
				}
				os.close();
				is.close();
			}
			UserGroupInformation.loginUserFromKeytab("bzsys_puyu@HADOOP.QIYI.COM", path);
			System.out.println("-----hbase kerberos virify seccess!-----");
		} catch (IOException e) {
			System.out.println("-----hbase kerberos virify fail!-----");
			e.printStackTrace();
		}
	}
	public static void initKerberosVerifyInJar() {
			System.setProperty("java.security.krb5.realm", "HADOOP.QIYI.COM");
			System.setProperty("java.security.krb5.kdc", "10.121.40.30");
			String path = "bzsys_puyu.keytab";
			InputStream is = TranswarpHBaseHeplerUtils.class.getClassLoader().getResourceAsStream(path);
			File keytab = new File(path);
			System.out.println("--initKerberosVerifyInJar method kerberos path--"+keytab.getAbsolutePath());
			try {
				if(!keytab.exists()){
					OutputStream os = new FileOutputStream(keytab);
					int bytesRead = 0;
					byte[] buffer = new byte[1024];
					while ((bytesRead = is.read(buffer, 0, 1024)) != -1) {
						os.write(buffer, 0, bytesRead);
					}
					os.close();
					is.close();
				}
				UserGroupInformation.loginUserFromKeytab("bzsys_puyu@HADOOP.QIYI.COM", path);
				System.out.println("-----hbase kerberos virify seccess!-----");
			} catch (IOException e) {
				System.out.println("-----hbase kerberos virify fail!-----");
				e.printStackTrace();
			}
		}
	public static void initKerberosVerifyJY(String kerberosPath) {

		System.setProperty("java.security.krb5.realm", "HADOOP.QIYI.COM");
		System.setProperty("java.security.krb5.kdc", "10.121.40.30");

		boolean isWindows = true;
		try {
			String os = System.getProperty("os.name").toLowerCase();
			if (os.indexOf("windows") == -1)
				isWindows = false;
			if (isWindows)
				UserGroupInformation.loginUserFromKeytab("bzsys@HADOOP.QIYI.COM", "E:/keytab/bzsys.keytab");
			else {
				UserGroupInformation.loginUserFromKeytab("bzsys@HADOOP.QIYI.COM", kerberosPath);
			}
			System.out.println("-----hbase kerberos virify seccess!-----");
		} catch (IOException e) {
			System.out.println("-----hbase kerberos virify fail!-----");
		}
	}
	
	public static void initKerberosVerify(String kerberosPath) {

		System.setProperty("java.security.krb5.realm", "HADOOP.QIYI.COM");
		System.setProperty("java.security.krb5.kdc", "10.121.40.30");

		boolean isWindows = true;
		try {
			String os = System.getProperty("os.name").toLowerCase();
			if (os.indexOf("windows") == -1)
				isWindows = false;
			if (isWindows)
				UserGroupInformation.loginUserFromKeytab("bzsys_puyu@HADOOP.QIYI.COM", kerberosPath);
			else {
				UserGroupInformation.loginUserFromKeytab("bzsys_puyu@HADOOP.QIYI.COM", kerberosPath);
			}
			System.out.println("-----hbase kerberos virify seccess!-----");
		} catch (IOException e) {
			System.out.println("-----hbase kerberos virify fail!-----");
		}
	}

	public static void clusterKerberosVerify() throws IOException {
		String kerberosPath = "/bzsys_puyu.keytab";
		File file = new File(kerberosPath);
		if (!file.exists()) {
			copyKerberosFileToLocal(kerberosPath);
		} else {
			System.out.println("-------kerberos file has exists!-----------");
		}
		initKerberosVerify(kerberosPath);
	}

	public static void copyKerberosFileToLocal(String kerberosPath) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataInputStream in = fs.open(new Path("/user/bzsys_puyu/oozie/bzsys_puyu.keytab"));
		OutputStream out = new FileOutputStream(kerberosPath);
		byte[] b = new byte[1024];
		int len = 0;
		while ((len = in.read(b, 0, 1024)) != -1) {
			out.write(b, 0, len);
		}
		in.close();
		out.close();
		fs.close();
	}

	public static void compactHTable(HBaseAdmin admin, String tableName, boolean closeAdmin) {
		try {
			admin.compact(tableName, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (closeAdmin)
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	public static long getHTableSize(String tableName) throws IOException {
		initKerberosVerify();
		Configuration conf = HBaseConfiguration.create();
		HTable htable = new HTable(conf, tableName);
		System.out.println("------init HTable " + tableName + "- connection success!------");
		RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(htable);
		long size = 0;
		Pair<byte[][], byte[][]> keys = htable.getStartEndKeys();
		for (int i = 0; i < keys.getFirst().length; i++) {
			HRegionLocation location = htable.getRegionLocation(keys.getFirst()[i], false);
			byte[] regionName = location.getRegionInfo().getRegionName();
			long regionSize = sizeCalculator.getRegionSize(regionName);
			size += regionSize;
			System.out.println(regionSize);
		}
		htable.close();
		return size;
	}
}
