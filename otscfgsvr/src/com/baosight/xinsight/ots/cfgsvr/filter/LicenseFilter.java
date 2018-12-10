package com.baosight.xinsight.ots.cfgsvr.filter;

import java.io.IOException;

import javax.servlet.Filter; 
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
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
    	HttpServletRequest webRequest = (HttpServletRequest)request;  
    	HttpServletResponse webResponse = (HttpServletResponse)response;  

		if (!ConfigUtil.getInstance().getLicenseMgr().isValid()) {
        	String requestURI = webRequest.getRequestURI().substring(webRequest.getRequestURI().indexOf("/", 1));  
    		if (requestURI.endsWith(CommonConstants.DEFAULT_HTML_SUFFIX) || requestURI.endsWith(CommonConstants.DEFAULT_JSP_SUFFIX)) {  
	        	LOG.error("###" + CommonConstants.AUTH_LICENSE_INVALID);
	            //System.out.println("###" + CommonConstants.AUTH_LICENSE_INVALID);              	
	        	
	        	String licenseStatus = ConfigUtil.getInstance().getLicenseMgr().getStatus();
	        	if (licenseStatus == null) {
	        		licenseStatus = "-1";
				} else {
					licenseStatus = licenseStatus.substring(0, licenseStatus.indexOf(","));
				}

	        	String forwoardPage = CommonConstants.DEFAULT_AUTH_FRAMELOGIN;
    			if (requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_LICENSEALARM)) {    				
    	        	//!!important, here use url redirect 
    				if (request.getParameterMap().get("forward") == null || request.getParameterMap().get("status") == null) {
    					webResponse.getWriter().write("<script language=javascript> window.location.href='" + CommonConstants.DEFAULT_AUTH_LICENSEALARM 
    							+ "?forward=" + forwoardPage + "&status=" + licenseStatus + "'; </script>");
    					return;
    				}
				} else {
					if (!requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_LOCALLOGIN) 
	    					&& !requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_FRAMELOGIN)) {    	        	
	    	        	//!!important, here use url redirect 
						webResponse.getWriter().write("<script language=javascript> window.location.href='" + CommonConstants.DEFAULT_AUTH_LICENSEALARM 
								+ "?forward=" + forwoardPage + "&status=" + licenseStatus + "'; </script>");
						return;
	    			} 
				}
    		} else if (requestURI.contains(CommonConstants.DEFAULT_API_PATH)) {
            	LOG.error("###" + CommonConstants.AUTH_LICENSE_INVALID);
                //System.out.println("###" + CommonConstants.AUTH_LICENSE_INVALID);
            	webResponse.sendError(HttpServletResponse.SC_FORBIDDEN, CommonConstants.AUTH_LICENSE_INVALID + "[" + ConfigUtil.getInstance().getLicenseMgr().getStatus() + "]");
	        	return;
			}    		
		}
		chain.doFilter(request, response);
	}
	
	@Override
	public void init(FilterConfig arg0) throws ServletException {
		
	} 
}