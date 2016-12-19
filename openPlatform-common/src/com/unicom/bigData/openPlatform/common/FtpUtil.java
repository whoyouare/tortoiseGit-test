package com.unicom.bigData.openPlatform.common;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FtpUtil {

	private final static Logger log = LoggerFactory.getLogger(FtpUtil.class);

	private FTPClient ftp;

	/**
	 * @param path
	 *            上传到ftp服务器哪个路径下
	 * @param addr
	 *            地址
	 * @param port
	 *            端口号
	 * @param username
	 *            用户名
	 * @param password
	 *            密码
	 * @return
	 * @throws Exception
	 */
	private boolean connect(String path, String addr, int port, String username, String password) throws Exception {
		boolean result = false;
		ftp = new FTPClient();
		int reply;
		ftp.connect(addr, port);
		ftp.login(username, password);
		ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
		reply = ftp.getReplyCode();
		if (!FTPReply.isPositiveCompletion(reply)) {
			ftp.disconnect();
			return result;
		}
		ftp.changeWorkingDirectory(path);
		result = true;
		return result;
	}

	/**
	 * 
	 * @desc 方法描述
	 * @param 参数描述
	 * @return 返回值描述
	 * @throws 异常描述
	 * @author:gary.qin
	 * @date: 2016-10-25
	 */
	private void upload(File file) throws Exception {
		if (file.isDirectory()) {
			ftp.makeDirectory(file.getName());
			ftp.changeWorkingDirectory(file.getName());
			String[] files = file.list();
			for (int i = 0; i < files.length; i++) {
				File file1 = new File(file.getPath() + "\\" + files[i]);
				if (file1.isDirectory()) {
					upload(file1);
					ftp.changeToParentDirectory();
				} else {
					File file2 = new File(file.getPath() + "\\" + files[i]);
					FileInputStream input = new FileInputStream(file2);
					ftp.storeFile(file2.getName(), input);
					input.close();
				}
			}
		} else {
			File file2 = new File(file.getPath());
			FileInputStream input = new FileInputStream(file2);
			ftp.storeFile(file2.getName(), input);
			input.close();
		}
	}

	/**
	 * 上传文件到服务器,新上传和断点续传
	 * 
	 * @param remotePath
	 *            远程文件名，在上传之前已经将服务器工作目录做了改变
	 * @param localPath
	 *            本地文件File句柄，绝对路径
	 * @param processStep
	 *            需要显示的处理进度步进值
	 * @param ftpClient
	 *            FTPClient引用
	 * @return
	 * @throws IOException
	 */
	private boolean uploadFile(String remotePath, File localPath, long remoteSize) throws Exception {
		// 显示进度的上传
		long step = localPath.length() / 100;
		long process = 0;
		long localreadbytes = 0L;
		RandomAccessFile raf = new RandomAccessFile(localPath, "r");
		OutputStream out = ftp.appendFileStream(remotePath);
		// 防止除0错误
		if (step == 0)
			step = 1;
		// 断点续传
		if (remoteSize > 0) {
			System.out.println("文件存在，可进行断点续传.");
			ftp.setRestartOffset(remoteSize);
			process = remoteSize / step;
			raf.seek(remoteSize);
			localreadbytes = remoteSize;
		}
		byte[] bytes = new byte[1024];
		int c;
		while ((c = raf.read(bytes)) != -1) {
			out.write(bytes, 0, c);
			localreadbytes += c;
			if (localreadbytes / step != process) {
				process = localreadbytes / step;
				if (process > 0 && process % 10 == 0) {
					if (process == 100) {
						System.out.println(process + "%");
						System.out.println(" 上传进度:" + process + "%");
					} else
						System.out.print(process + "%");
				} else {
					System.out.print(".");
				}
			}
		}
		out.flush();
		raf.close();
		out.close();
		boolean result = ftp.completePendingCommand();
		if (result) {
			log.info("上传文件完成");
			return true;
		} else {
			log.error("上传文件失败");
			return false;
		}
	}

	public static void main(String[] args) throws Exception {
		FtpUtil t = new FtpUtil();
		t.connect("/data/test", "10.15.244.208", 21, "userftp", "admin");
		File file = new File("D:\\ODGS.txt");
		t.upload(file);
		//t.uploadFile("/data/test/ODGS.txt", file, 0l);

	}
}