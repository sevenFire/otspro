package com.baosight.xinsight.ots.rest.filter;

import java.io.IOException;
import java.net.InetAddress;

import javax.servlet.Filter; 
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.log4j.NDC;

public class Log4jNdcFilter implements Filter {

	@Override
	public void destroy() {
		
	}
	
	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {		
		// 把网络地址放入 NDC 中 . 那么在在 layout pattern 中通过使用 %x，就可在每条日之中增加网络地址的信息 . 
		InetAddress inetAddr = InetAddress.getLocalHost();
		String hostName = inetAddr.getHostName();
		NDC.push(hostName); 
		chain.doFilter(request, response); 
		
		// 从 NDC 的堆栈中删除网络地址 . 
		NDC.pop();		
	}
	
	@Override
	public void init(FilterConfig arg0) throws ServletException {
		
	} 
}