package com.baosight.xinsight.ots.rest.filter;

import java.io.IOException;

import javax.servlet.Filter; 
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;


public class LicenseFilter implements Filter {
	private static final Logger LOG = Logger.getLogger(LicenseFilter.class);

	@Override
	public void destroy() {
		
	}
	
	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {		
    	HttpServletResponse webResponse = (HttpServletResponse)response;  

		if (!ConfigUtil.getInstance().getLicenseMgr().isValid()) {
        	LOG.error("###" + CommonConstants.AUTH_LICENSE_INVALID);
            //System.out.println("###" + CommonConstants.AUTH_LICENSE_INVALID);            
			webResponse.sendError(HttpServletResponse.SC_FORBIDDEN, CommonConstants.AUTH_LICENSE_INVALID + "[" + ConfigUtil.getInstance().getLicenseMgr().getStatus() + "]");

			return;
		}
		chain.doFilter(request, response);
	}
	
	@Override
	public void init(FilterConfig arg0) throws ServletException {
		
	} 
}