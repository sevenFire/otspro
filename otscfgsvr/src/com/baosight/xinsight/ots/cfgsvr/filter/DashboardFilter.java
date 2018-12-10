package com.baosight.xinsight.ots.cfgsvr.filter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.apache.log4j.Logger;

import com.baosight.xinsight.auth.AuthTokenManager;
import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.model.LoginModel;
import com.baosight.xinsight.model.UserInfo;
import com.baosight.xinsight.ots.cfgsvr.common.RestConstants;
import com.baosight.xinsight.ots.cfgsvr.util.ConfigUtil;


@Provider
public class DashboardFilter implements Filter {  
	private static final Logger LOG = Logger.getLogger(DashboardFilter.class);
	  
    @Override  
    public void destroy() {
  
    }  
  
    @Override  
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {

    	HttpServletRequest webRequest = (HttpServletRequest)request;  
    	HttpServletResponse webResponse = (HttpServletResponse)response;  
    	AuthTokenManager tokenManager = AuthTokenManager.createInstance(ConfigUtil.getInstance().getAuthServerAddr()); 

    	// check dashboard page
    	String requestURI = webRequest.getRequestURI().substring(webRequest.getRequestURI().indexOf("/", 1));  
    	if (requestURI.contains(CommonConstants.DEFAULT_DASHBOARD_PATH)) {   		
           	String method = webRequest.getMethod();
           	
    		if (requestURI.endsWith(CommonConstants.DEFAULT_HTML_SUFFIX) || requestURI.endsWith(CommonConstants.DEFAULT_JSP_SUFFIX)) {    			
            	String absolutePath = webRequest.getRequestURL().toString(); 
            	LOG.debug("Method:"+ method + ", Url:" + absolutePath); 
            	            	
	    		// if not login/index page, check the session. otherwise, just ignore.  
            	Boolean checkSession = true;            	
        		if (ConfigUtil.getInstance()._isDebugModeBoolean) {
        			if(requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_FRAMELOGIN) 
        					|| requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_FRAMEHOME)) {
		            	LOG.warn("Running mode:"+ ConfigUtil.getInstance()._isDebugModeBoolean); 
						webResponse.sendRedirect(webRequest.getContextPath() + CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_LOCALLOGIN); 
						return;
					}
        			
            		if(requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_DASHBOARD_HOME) 
            				|| requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_NOAUTHPAGE) 
            				|| requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_NOSESSION) 
            				|| requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_LICENSEALARM) 
            				|| requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_LOCALLOGIN))
						checkSession = false;
				} else {
					if(requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_LOCALLOGIN)) {
		            	LOG.warn("Running mode:"+ ConfigUtil.getInstance()._isDebugModeBoolean); 
						webResponse.sendRedirect(webRequest.getContextPath() + CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_FRAMELOGIN); 
						return;
					}
					
					if(requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_DASHBOARD_HOME) 
							|| requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_NOAUTHPAGE) 
							|| requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_NOSESSION) 
							|| requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_LICENSEALARM) 
							|| requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_FRAMELOGIN) 
							|| requestURI.endsWith(CommonConstants.DEFAULT_DASHBOARD_PATH + CommonConstants.DEFAULT_AUTH_FRAMEHOME))
						checkSession = false;
				}
        		
            	if(checkSession) {            		

	                // get session. if not, set it null by parameter false  
	            	Object tokenObject = null;
	                HttpSession session = webRequest.getSession(false);                
	                if(session != null) {  
	                	tokenObject = session.getAttribute(CommonConstants.SESSION_TOKEN_KEY);  
	                }	                	                
	                if (tokenObject != null) {
	                	UserInfo userInfo = tokenManager.getUser(tokenObject.toString());
						if (userInfo == null) {						
			            	LOG.warn("no valid token attribute! the token is:" + tokenObject.toString()); 
													
							String forwoardPage;
							if (ConfigUtil.getInstance()._isDebugModeBoolean) {
								forwoardPage = CommonConstants.DEFAULT_AUTH_LOCALLOGIN;
							} else {
								forwoardPage = CommonConstants.DEFAULT_AUTH_FRAMELOGIN;
							}
		    	        	//!!important, here use url redirect 
							webResponse.getWriter().write("<script language=javascript> window.location.href='" + CommonConstants.DEFAULT_AUTH_NOSESSION 
									+ "?forward=" + forwoardPage + "'; </script>");
							return;	
						}
//						if (!userInfo.getUserName().equalsIgnoreCase(CommonConstants.DEFAULT_API_USER_ADMIN)) {						
//							LOG.warn("no auth if not admin user!"); 
//							
//							String forwoardPage;
//							if (ConfigUtil.getInstance()._isDebugModeBoolean) {
//								forwoardPage = Constants.DEFAULT_AUTH_LOCALLOGIN;
//							} else {
//								forwoardPage = Constants.DEFAULT_AUTH_FRAMEHOME;
//							}
//		    	        	//!!important, here use url redirect 
//							webResponse.getWriter().write("<script language=javascript> window.location.href='" + Constants.DEFAULT_AUTH_NOAUTHPAGE 
//									+ "?forward=" + forwoardPage + "'; </script>");
//							return;
//						}
	                } else {
						LOG.warn("no valid session attribute!"); 
						
						String forwoardPage;
						if (ConfigUtil.getInstance()._isDebugModeBoolean) {
							forwoardPage = CommonConstants.DEFAULT_AUTH_LOCALLOGIN;
						} else {
							forwoardPage = CommonConstants.DEFAULT_AUTH_FRAMELOGIN;
						}
	    	        	//!!important, here use write redirect, both for model dialog jsp and normal jsp 							
						webResponse.getWriter().write("<script language=javascript> window.location.href='" + CommonConstants.DEFAULT_AUTH_NOSESSION 
								+ "?forward=" + forwoardPage + "'; </script>");
						return;		                    
					}
	            }
    		} else if (requestURI.endsWith(RestConstants.DEFAULT_API_PATH_LOCALLOGIN)) {  //if dashboard login action          	
            	String absolutePath = webRequest.getRequestURL().toString(); 
            	LOG.debug("Method:"+ method + ", Url:" + absolutePath); 
            	
	  			try {
					ServletInputStream input = webRequest.getInputStream();
					BufferedReader br = new BufferedReader(new InputStreamReader(input));								

		            String line = null;
		            StringBuffer sb = new StringBuffer();
		            while ((line = br.readLine()) != null) {
		                sb.append(line);
		            }
		            
		            LoginModel model = null;
		            try {
		            	String in = sb.toString();
		            	model = LoginModel.toClass(in);	 
					} catch (Exception e) {
						//e.printStackTrace();						
						LOG.error(CommonConstants.AUTH_EXCEP_SYNTAX_ERR);
						webResponse.sendError(Response.Status.UNAUTHORIZED.getStatusCode(), CommonConstants.AUTH_EXCEP_SYNTAX_ERR);
						return;
					}
		            
			        if (tokenManager.check(model)) {
				        UserInfo user = tokenManager.getUser(model);
		            	if (user != null) {
				            LOG.debug(model.getFullName() + ":" + CommonConstants.AUTH_AUTHENTICATED);
			            
				            HttpSession session = webRequest.getSession();
				            session.setAttribute(CommonConstants.SESSION_TENANT_KEY, model.getTenant());
				            session.setAttribute(CommonConstants.SESSION_USERNAME_KEY, model.getUsername());
				            session.setAttribute(CommonConstants.SESSION_TOKEN_KEY, tokenManager.getToken(model));
				            session.setAttribute(CommonConstants.SESSION_TENANTID_KEY, user.getTenantId());
				            session.setAttribute(CommonConstants.SESSION_USERID_KEY, user.getUserId());
				            
				            webResponse.sendError(Response.Status.OK.getStatusCode());
				    		return;		
						} else {
							throw new IOException(CommonConstants.AUTH_EXCEP_ERR_CRED);
						}           
		            } else {
						throw new IOException(CommonConstants.AUTH_EXCEP_ERR_CRED);
					}	  			
				} catch (IOException e) {
					e.printStackTrace();
					
		    		LOG.error(CommonConstants.AUTH_EXCEP_ERR_CRED);	    		
					webResponse.sendError(Response.Status.UNAUTHORIZED.getStatusCode(), CommonConstants.AUTH_EXCEP_ERR_CRED);
					return;
				}
	        }
	        else if (requestURI.endsWith(RestConstants.DEFAULT_API_PATH_LOCALLOGOUT)) {  //if dashboard logout action          	
            	String absolutePath = webRequest.getRequestURL().toString(); 
            	LOG.debug("Method:"+ method + ", Url:" + absolutePath); 
            	
            	HttpSession session = webRequest.getSession(false);
            	if (session != null) {
		            session.removeAttribute(CommonConstants.SESSION_TOKEN_KEY);
            		session.invalidate();
				}
				
				webResponse.sendError(Response.Status.OK.getStatusCode());
				return;
	        }	        
    	}
    	
        chain.doFilter(webRequest, webResponse);    
    }

	@Override
	public void init(FilterConfig config) throws ServletException {
		
	}  
}  
