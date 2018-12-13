<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html >
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=gbk" />
	<title>mob字段的读写demo举例--xInsight big-data</title>
	<link rel="shortcut icon" href="../images/icon.ico"/>	
	<script type="text/javascript" src="scripts/shCore.js"></script>
    <!-- 下面 pre 中 brush 是 java，这里就要导入 shBrushJava 的 js 文件 -->
    <!-- scripts 目录下还有很多的 shBrushXxxx.js 文件 -->
    <script type="text/javascript" src="scripts/shBrushJava.js"></script>
    <link type="text/css" rel="stylesheet" href="styles/shCore.css"/>
    <!-- 这里使用 eclipse 外观，styles 目录下还有很多的外观 -->
	<link type="text/css" rel="stylesheet" href="styles/shThemeEclipse.css"/>
	<script type="text/javascript">
		SyntaxHighlighter.config.clipboardSwf = 'scripts/clipboard.swf';
		SyntaxHighlighter.all();
	</script>
</head>

<style type="text/css">
/** 改一下字号大小，否则格式化后的代码会很大 */
.syntaxhighlighter div { font-size:12px !important; }
</style>

<body>
<p>mob字段的读写demo举例</p>
<div style="width:80%;border:1px solid #EDEDED;padding:10px;background:#F7F7F7;color:#4F4F4F;">
<pre class="brush: java;">
package com.baosight.xinsight.ots.rest.test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;

public class MobTest {
	public static void main(String[] args) throws Exception {
		MobTest to = new MobTest();

		to.putRecord("168.2.4.54:8080", "oss_13_2", "range",
		        "2015-09-06_104102", "image", "D:\\2015-09-06_104102.jpg");
		to.getRecord("168.2.4.54:8080", "oss_13_2", "range",
				"2015-09-06_104102", "image", "D:\\2015-09-06_104102_g.jpg");
	}

	public String getRecord(String addr, String tableName, String hashkey,
			String rangekey, String columnName, String outputFile) {
		try {
			URL urlObj = new URL("http://" + addr + "/otsrest/api/record/file/"
					+ tableName + "?hash_key=" + hashkey + "&range_key="
					+ rangekey + "&column=" + columnName);
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();

			//注意这里要存入用户名密码的base64编码YmFva2FuZzpieXM=代表baokang，现场测试根据情况修改
			http_conn.setRequestProperty("Authorization",
					"Basic YWRtaW5Ab3NzOmFkbWlu");
			http_conn.setDoOutput(true);
			http_conn.setUseCaches(false);

			http_conn.setConnectTimeout(20 * 1000);
			http_conn.setReadTimeout(20 * 1000);

			http_conn.setRequestMethod("GET");
			http_conn.setRequestProperty("Content-Type",
					"application/octet-stream");
			http_conn.connect();

			InputStream in = http_conn.getInputStream();
			FileOutputStream fos = new FileOutputStream(outputFile);
			byte[] b = new byte[1026];
			int count = 0;
			while ((count = in.read(b)) != -1) {
				fos.write(b, 0, count);

				// 如果需要在ots页面查看到内容，建议转为16进制编码读写
				// String strContent = new String(b);
				// byte[] bytes = hexStringToBytes(strContent);
				// fos.write(bytes);
			}
			in.close();
			fos.close();

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	public String putRecord(String addr, String tableName, String hashkey,
			String rangekey, String columnName, String inputFile) {
		try {
			URL urlObj = new URL("http://" + addr + "/otsrest/api/record/file/"
					+ tableName + "?hash_key=" + hashkey + "&range_key="
					+ rangekey + "&column=" + columnName);
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			http_conn.setRequestMethod("POST");
			//这里必须为octet-stream
			http_conn.setRequestProperty("Content-Type",
					"application/octet-stream");
			//注意这里要存入用户名密码的base64编码YmFva2FuZzpieXM=代表baokang，现场测试根据情况修改
			http_conn.setRequestProperty("Authorization",
					"Basic YWRtaW5Ab3NzOmFkbWlu");

			OutputStream dos = http_conn.getOutputStream();
			FileInputStream fis = new FileInputStream(inputFile);
			int count = 0;
			byte[] buffer = new byte[1026];
			while ((count = fis.read(buffer)) != -1) {
				dos.write(buffer, 0, count);

				// 如果需要在ots页面查看到内容，建议转为16进制编码读写
				// StringBuffer bufferContent = bytesToString(buffer);
				// dos.write(bufferContent.toString().getBytes());
			}
			fis.close();
			dos.flush();
			dos.close();

			// 获得响应状态
			int responseCode = http_conn.getResponseCode();
			if (HttpURLConnection.HTTP_CREATED == responseCode) {// 连接成功
				BufferedReader in = new BufferedReader(new InputStreamReader(
						(InputStream) http_conn.getInputStream(), "UTF-8"));
				String returnData = in.readLine();
				System.out.println(returnData);
				in.close();
			}

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 将字符串形式表示的十六进制数转换为byte数组
	 */
	public static byte[] hexStringToBytes(String hexString) {
		hexString = hexString.toLowerCase();
		String[] hexStrings = hexString.split(" ");
		byte[] bytes = new byte[hexStrings.length];
		for (int i = 0; i < hexStrings.length; i++) {
			char[] hexChars = hexStrings[i].toCharArray();
			bytes[i] = (byte) (charToByte(hexChars[0]) << 4 
					| charToByte(hexChars[1]));
		}
		return bytes;
	}

	/**
	 * 将byte数组转换为字符串形式表示的十六进制数方便查看
	 */
	public static StringBuffer bytesToString(byte[] bytes) {
		StringBuffer sBuffer = new StringBuffer();
		for (int i = 0; i < bytes.length; i++) {
			String s = Integer.toHexString(bytes[i] & 0xff);
			if (s.length() < 2)
				sBuffer.append('0');
			sBuffer.append(s + " ");
		}
		return sBuffer;
	}

	private static byte charToByte(char c) {
		return (byte) "0123456789abcdef".indexOf(c);
		// 或 return (byte) "0123456789ABCDEF".indexOf(c);
	}
}

</pre>
</div>
</body>
</html>