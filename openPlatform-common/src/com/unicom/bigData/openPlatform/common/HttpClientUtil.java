package com.unicom.bigData.openPlatform.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @name HttpClientUtil.java
 * @author gary.qin
 * @version 2016-10-24
 */
public class HttpClientUtil {

	private final static Logger log = LoggerFactory.getLogger(HttpClientUtil.class);

	private static final String HTTP_REQUEST_NAME_VALUE_DELIMITER = "&";

	public static final String DEFAULT_CONTENT_TYPE = "application/x-www-form-urlencoded";

	/**
	 * 默认字符集
	 */
	public static final String DEFAULT_CHAR_SET = "UTF-8";

	/**
	 * 默认读响应流缓冲大小
	 */
	private static final int DEFAULT_STREAM_BUFFER_SIZE = 1024;
	/**
	 * 默认一分钟超时
	 */
	public static final int DEFAULT_REQUEST_TIMEOUT = 60 * 1000;

	private static HttpClientUtil instance = null;

	private MultiThreadedHttpConnectionManager connectionManager;

	private HttpClientUtil() {
		connectionManager = new MultiThreadedHttpConnectionManager();
		HttpConnectionManagerParams params = new HttpConnectionManagerParams();
		params.setDefaultMaxConnectionsPerHost(10);
		params.setMaxTotalConnections(50);
		connectionManager.setParams(params);
	}

	public static synchronized HttpClientUtil getInstance() {
		if (instance == null) {
			instance = new HttpClientUtil();
		}
		return instance;
	}

	public String getResposeByProxy(String url, Map<String, String> params, Map<String, String> headers, int timeout,
			boolean useStream, String charSet, String ip, Integer port, String userName, String password) {

		return getResposeByProxy(new GetMethod(url), params, headers, timeout, useStream, charSet, ip, port, userName,
				password);
	}

	// 增加代理功能
	public String getResposeByProxy(HttpMethod httpMethod, Map<String, String> params, Map<String, String> headers,
			int timeout, boolean useStream, String charSet, String ip, Integer port, String userName, String password) {

		long start = System.currentTimeMillis();
		String response = null;

		String url = null;

		try {
			url = httpMethod.getURI().toString();
			HttpClient client = new HttpClient();

			if (ip != null && port != null) {
				// 设置HTTP代理IP和端口
				client.getHostConfiguration().setProxy(ip, port);
				// 代理认证
				if (userName != null) {
					UsernamePasswordCredentials creds = new UsernamePasswordCredentials(userName, password);
					client.getState().setProxyCredentials(AuthScope.ANY, creds);
				}
			}
			// Connection & Read timeout configuration
			if (timeout > 0) {
				client.getHttpConnectionManager().getParams().setConnectionTimeout(timeout);
				client.getHttpConnectionManager().getParams().setSoTimeout(timeout);
			} else {
				client.getHttpConnectionManager().getParams().setConnectionTimeout(DEFAULT_STREAM_BUFFER_SIZE);
				client.getHttpConnectionManager().getParams().setSoTimeout(DEFAULT_STREAM_BUFFER_SIZE);
			}

			client.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET, DEFAULT_CHAR_SET);

			// Set HTTP method's headers.
			httpMethod.addRequestHeader("Connection", "close");

			if (headers != null && headers.size() > 0) {
				Set<Map.Entry<String, String>> entrys = headers.entrySet();
				for (Map.Entry<String, String> entry : entrys) {
					httpMethod.addRequestHeader(entry.getKey(), entry.getValue());
				}
			}

			if (charSet != null) {
				httpMethod.getParams().setContentCharset(charSet);
			} else {
				httpMethod.getParams().setContentCharset(DEFAULT_CHAR_SET);
			}

			// Write to request body suppot PUT and POST
			if (httpMethod instanceof EntityEnclosingMethod) {
				StringBuilder pairs = new StringBuilder();
				if (params != null && params.size() != 0) {
					Set<Map.Entry<String, String>> entrys = params.entrySet();
					for (Map.Entry<String, String> entry : entrys) {
						pairs.append(entry.getKey()).append("=").append(entry.getValue())
								.append(HTTP_REQUEST_NAME_VALUE_DELIMITER);
					}
				}
				String content = StringUtils.removeEnd(pairs.toString(), HTTP_REQUEST_NAME_VALUE_DELIMITER);
				if (content.length() > 0) {
					RequestEntity entity = new ByteArrayRequestEntity(content.getBytes(DEFAULT_CHAR_SET),
							DEFAULT_CONTENT_TYPE);
					((EntityEnclosingMethod) httpMethod).setRequestEntity(entity);
				}
			}

			int status = client.executeMethod(httpMethod);
			log.debug("HttpMethod[" + httpMethod.getName() + "] to URL[" + httpMethod.getURI() + "] response status["
					+ status + "]");

			if (status == HttpStatus.SC_OK) {// Success
				if (httpMethod instanceof HttpMethodBase) {
					String charset = ((HttpMethodBase) httpMethod).getResponseCharSet();
					log.debug("charset[" + charset + "]");
				}
				if (useStream) {
					response = readLargeResponse(null, httpMethod);
				} else {
					response = httpMethod.getResponseBodyAsString();
				}
			} else if ((status == HttpStatus.SC_MOVED_TEMPORARILY) || (status == HttpStatus.SC_MOVED_PERMANENTLY)
					|| (status == HttpStatus.SC_SEE_OTHER) || (status == HttpStatus.SC_TEMPORARY_REDIRECT)) {// redirection
				response = redirect(httpMethod, null, client);
			} else if ((status == HttpStatus.SC_FORBIDDEN)) {
				response = String.valueOf(HttpStatus.SC_FORBIDDEN);
			} else {
				log.warn("HttpMethod[" + httpMethod.getName() + "] to URL[" + httpMethod.getURI()
						+ "] response failed! status[" + status + "]");
			}
		} catch (Exception e) {
			log.info("Http request to [" + url + "] failed!");
			log.warn(e.getMessage(), e);
			throw new RuntimeException();
		} finally {
			httpMethod.releaseConnection();
			long end = System.currentTimeMillis();
			log.debug("HttpMethod[" + httpMethod.getName() + "] to URL[" + url + "] finish cost [" + (end - start)
					+ "] ms");
		}
		return response;

	}

	// 重载
	public String getResponse(String url, Map<String, String> params, Map<String, String> headers, int timeout,
			boolean useStream, String charSet) {

		return getResposeByProxy(new GetMethod(url), params, headers, timeout, useStream, charSet, null, null, null,
				null);
	}

	/**
	 * 
	 * @desc 方法描述
	 * @param 参数描述
	 * @return 返回值描述
	 * @throws 异常描述
	 * @author:gary.qin
	 * @date: 2016-10-24
	 */
	public String getResponse(HttpMethod httpMethod, Map<String, String> params, Map<String, String> headers,
			int timeout, boolean useStream, String charSet) {

		return getResposeByProxy(httpMethod, params, headers, timeout, useStream, charSet, null, null, null, null);
	}

	private String redirect(HttpMethod httpMethod, String response, HttpClient client) throws IOException {
		int status;
		Header header = httpMethod.getResponseHeader("location");
		if (header != null) {
			String location = header.getValue();
			if ((location == null) || (location.equals(""))) {
				location = "/";
			}
			HttpMethod redirect = new GetMethod(location);// Just use GET method
			try {
				status = client.executeMethod(redirect);
				response = redirect.getResponseBodyAsString();
				log.info("Redirection to [" + location + "] respose status[" + status + "] message[" + response + "]");
			} finally {
				redirect.releaseConnection();
			}
		} else {
			log.warn("No redirection location found.");
		}
		return response;
	}

	private static String readLargeResponse(String response, HttpMethod httpMethod) {
		InputStream in = null;
		ByteArrayOutputStream baos = null;
		try {
			String charset = httpMethod.getParams().getContentCharset();
			// System.out.println("charset:" + charset);
			in = httpMethod.getResponseBodyAsStream();
			baos = new ByteArrayOutputStream();
			byte[] buffer = new byte[DEFAULT_STREAM_BUFFER_SIZE];
			IOUtils.copyLarge(in, baos, buffer);
			response = new String(baos.toByteArray(), charset == null ? DEFAULT_CHAR_SET : charset);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(baos);
		}
		return response;
	}

	private Map<String, String> getHostHttpHeaders(String clientIP) {

		Map<String, String> headers = new HashMap<String, String>();
		headers.put("X-Real-IP", clientIP);
		headers.put("REMOTE-HOST", clientIP);
		headers.put("Proxy-Client-IP", clientIP);
		headers.put("X-Forwarded-For", clientIP);
		headers.put("HTTP_CLIENT_IP", clientIP);
		headers.put("Host", "10.11.50.171");// 

		return headers;
	}

	/**
	 * @since 1.3.0
	 * @param url
	 * @return
	 */
	public String getDeleteResponse(String url) {
		return getResponse(new DeleteMethod(url), null, null, DEFAULT_REQUEST_TIMEOUT, false, DEFAULT_CHAR_SET);
	}

	/**
	 * @since 1.3.0
	 * @param url
	 * @param timeout
	 * @return
	 */
	public String getDeleteResponse(String url, int timeout) {
		return getResponse(new DeleteMethod(url), null, null, timeout, false, DEFAULT_CHAR_SET);
	}

	/**
	 * @since 1.3.0
	 * @param url
	 * @param timeout
	 * @return
	 */
	public String getDeleteResponse(String url, int timeout, Map<String, String> headers) {
		return getResponse(new DeleteMethod(url), null, headers, timeout, false, DEFAULT_CHAR_SET);
	}

	/**
	 * @since 1.3.0
	 * @param url
	 * @param params
	 * @return
	 */
	public String getPutResponse(String url, Map<String, String> params) {
		return getPutResponse(url, params, null, DEFAULT_REQUEST_TIMEOUT);
	}

	/**
	 * @since 1.3.0
	 * @param url
	 * @param params
	 * @param timeout
	 * @return
	 */
	public String getPutResponse(String url, Map<String, String> params, int timeout) {
		return getPutResponse(url, params, null, timeout);
	}

	/**
	 * @since 1.3.0
	 * @param url
	 * @param params
	 * @param timeout
	 * @return
	 */
	public String getPutResponse(String url, Map<String, String> params, Map<String, String> headers, int timeout) {
		return getResponse(new PutMethod(url), params, headers, timeout, false, DEFAULT_CHAR_SET);
	}

	public String getGetResponse(String url, boolean useStream) {

		return getGetResponseWithHost(url, useStream, null);
	}

	public String getGetResponseWithHost(String url, boolean useStream, String clientIP) {

		Map<String, String> headers = null;
		if (clientIP != null && !clientIP.equals("")) {
			headers = getHostHttpHeaders(clientIP);
		}
		return getResponse(new GetMethod(url), null, headers, DEFAULT_REQUEST_TIMEOUT, useStream, DEFAULT_CHAR_SET);
	}

	public String getPostResponse(String url, Map<String, String> params, Integer timeOut) {
		return getPostResponseOnCharset(url, params, false, DEFAULT_CHAR_SET, timeOut);
	}

	public String getPostResponse(String url, Map<String, String> params) {
		return getPostResponseOnCharset(url, params, false, DEFAULT_CHAR_SET, DEFAULT_REQUEST_TIMEOUT);
	}

	public String getPostResponse(String url, Map<String, String> params, boolean useStream) {
		return getPostResponseOnCharset(url, params, useStream, DEFAULT_CHAR_SET, DEFAULT_REQUEST_TIMEOUT);
	}

	public String getPostResponseOnCharset(String url, Map<String, String> params, boolean useStream, String charSet,
			int timeout) {
		return getPostResponseOnCharsetWithHost(url, params, null, useStream, charSet, timeout);
	}

	public String getPostResponseWithHost(String url, Map<String, String> params, boolean useStream, String clientIP) {

		return getPostResponseOnCharsetWithHost(url, params, clientIP, useStream, DEFAULT_CHAR_SET,
				DEFAULT_REQUEST_TIMEOUT);
	}

	public String getPostResponseOnCharsetWithHost(String url, Map<String, String> params, String clientIP,
			boolean useStream, String charSet, int timeout) {

		Map<String, String> headers = null;
		if (clientIP != null && !clientIP.equals("")) {
			headers = getHostHttpHeaders(clientIP);
		}
		return getResponse(new PostMethod(url), params, headers, DEFAULT_REQUEST_TIMEOUT, useStream, DEFAULT_CHAR_SET);
	}

	/**
	 * 通过url的方式获得返回的页面代码
	 * 
	 * @param url
	 * @return
	 */
	public static String httpRequest(String url) {
		String response = null;
		InputStream in = null;
		ByteArrayOutputStream baos = null;
		try {
			URL requestUrl = new URL(url);
			in = requestUrl.openStream();
			requestUrl.openConnection();
			baos = new ByteArrayOutputStream();
			byte[] buffer = new byte[DEFAULT_STREAM_BUFFER_SIZE];
			IOUtils.copyLarge(in, baos, buffer);
			response = new String(baos.toByteArray(), DEFAULT_CHAR_SET);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(baos);
		}
		return response;
	}

	/**
	 * 通过httpClient的headmethod方法获得url的状态
	 * 
	 * @param url
	 * @return
	 */
	public int getUrlStatus(String url) {
		HttpClient client = new HttpClient(connectionManager);
		client.getParams().setBooleanParameter("http.protocol.expect-continue", false);
		HeadMethod headMethod = new HeadMethod(url);
		headMethod.addRequestHeader("Connection", "close");
		int status = HttpStatus.SC_NOT_FOUND;
		try {
			status = client.executeMethod(headMethod);
		} catch (HttpException e) {
			log.info("please check out your get url address : " + url);
		} catch (IOException e) {
			log.info("IOException when get from your url : " + url);
		} finally {
			headMethod.releaseConnection();
		}
		return status;
	}
}
