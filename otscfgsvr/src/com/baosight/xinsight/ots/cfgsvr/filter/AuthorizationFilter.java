package com.baosight.xinsight.ots.cfgsvr.filter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.apache.log4j.Logger;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.model.LoginModel;
import com.baosight.xinsight.model.UserInfo;
import com.baosight.xinsight.ots.cfgsvr.util.ConfigUtil;
import com.baosight.xinsight.auth.AuthTokenManager;
import com.sun.jersey.core.util.Base64;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

@Provider
public class AuthorizationFilter implements ContainerRequestFilter {
	private static final Logger LOG = Logger.getLogger(AuthorizationFilter.class);
	
	@Context
    HttpServletRequest webRequest;
	
	@Override
    public ContainerRequest filter(ContainerRequest request) {        
    	Response response = null;
    	AuthTokenManager tokenManager = AuthTokenManager.createInstance(ConfigUtil.getInstance().getAuthServerAddr()); 
    	
    	String method = request.getMethod();
    	String absolutePath = request.getAbsolutePath().toString();
    	LOG.debug("Method:"+ method + ", Url:" + absolutePath);

    	// Cross domain filter check    	
    	if(method.equals(CommonConstants.AUTH_CROSSDOMAINN_CHECK)) {
    		LOG.debug("Cross domain request!");
            throw new WebApplicationException(Response.Status.OK);
        }    	
    	
      	// check rest API
    	if (absolutePath.contains(CommonConstants.DEFAULT_API_PATH)) {
    		String token = request.getQueryParameters().getFirst(CommonConstants.SESSION_TOKEN_KEY);
    		if (null == token){
    			token = request.getHeaderValue(CommonConstants.SESSION_TOKEN_KEY);
    		}
    		if (token != null) {
            	UserInfo userInfo = tokenManager.getUser(token);
				if (userInfo == null) {						
	            	LOG.warn(token + CommonConstants.DEFAULT_COLON_SPLIT + CommonConstants.AUTH_EXCEP_INVALID_TOKEN);					
					response = Response.status(Response.Status.UNAUTHORIZED).type(MediaType.TEXT_PLAIN).entity(CommonConstants.AUTH_EXCEP_INVALID_TOKEN).build();
		    		throw new WebApplicationException(response);
				}
								
				//url token was valid
				webRequest.setAttribute(CommonConstants.SESSION_TENANT_KEY, userInfo.getTenantName());
		        webRequest.setAttribute(CommonConstants.SESSION_USERNAME_KEY, userInfo.getUserName());
				webRequest.setAttribute(CommonConstants.SESSION_USERID_KEY, userInfo.getUserId());
            	webRequest.setAttribute(CommonConstants.SESSION_TENANTID_KEY, userInfo.getTenantId());
				webRequest.setAttribute(CommonConstants.SESSION_TOKEN_KEY, token);
			} else {
				// check rest API, Extract authentication credentials
	    		String authentication = request.getHeaderValue(ContainerRequest.AUTHORIZATION);	
	    		if (authentication != null) {		    	
				    if (!authentication.startsWith(CommonConstants.AUTH_BASIC_TYPE)) {
				    	// "Only HTTP Basic authentication is supported"
				    	LOG.error(CommonConstants.AUTH_EXCEP_ONLY_BASIC);
				    	response = Response.status(Response.Status.UNAUTHORIZED).type(MediaType.TEXT_PLAIN).entity(CommonConstants.AUTH_EXCEP_ONLY_BASIC).build();
				    	throw new WebApplicationException(response);
				    	// additional checks should be done here 
				    }			    
		
				    String authArray = authentication.substring(CommonConstants.AUTH_BASIC_TYPE.length());
				    String[] values = new String(Base64.base64Decode(authArray)).split(CommonConstants.DEFAULT_COLON_SPLIT);
				    if (values.length < CommonConstants.AUTH_USER_LENGTH) {
				    	// "Invalid syntax for username and password"
				    	LOG.error(CommonConstants.AUTH_EXCEP_SYNTAX_ERR);
				    	response = Response.status(Response.Status.BAD_REQUEST).type(MediaType.TEXT_PLAIN).entity(CommonConstants.AUTH_EXCEP_SYNTAX_ERR).build();
				    	throw new WebApplicationException(response);
				    }
				    			   
				    String input_userinfo = values[0];
				    String password = values[1]; 
				    String username = input_userinfo;
				    String tenant = username;
			        if ((input_userinfo == null) || (password == null)) {
			        	// "Missing username or password"
			        	LOG.error(CommonConstants.AUTH_EXCEP_MISS_CRED);
			        	response = Response.status(Response.Status.BAD_REQUEST).type(MediaType.TEXT_PLAIN).entity(CommonConstants.AUTH_EXCEP_MISS_CRED).build();
						throw new WebApplicationException(response);            
			        }
			        // Validate the extracted credentials
			        int pos = input_userinfo.indexOf(CommonConstants.DEFAULT_DOMAIN_SPLIT);
			        if (pos >= 0) {
			        	username = input_userinfo.substring(0, pos);
			        	tenant = input_userinfo.substring(pos + 1);
					} else {
			        	LOG.error(input_userinfo + CommonConstants.DEFAULT_COLON_SPLIT + CommonConstants.AUTH_NOT_AUTHENTICATED);
			            //System.out.println(input_userinfo + CommonConstants.DEFAULT_COLON_SPLIT + CommonConstants.AUTH_NOT_AUTHENTICATED);            
			           	response = Response.status(Response.Status.UNAUTHORIZED).type(MediaType.TEXT_PLAIN).entity(CommonConstants.AUTH_EXCEP_ERR_CRED).build();
			    		throw new WebApplicationException(response);
					}
			        	        
			        LoginModel model = new LoginModel(tenant, username, password);
			        if (tokenManager.check(model)) {
			             
			            webRequest.setAttribute(CommonConstants.SESSION_TENANT_KEY, tenant);
			            webRequest.setAttribute(CommonConstants.SESSION_USERNAME_KEY, username);			            

			            UserInfo user = tokenManager.getUser(model);
			            if (user != null) {
				            //System.out.println(model.getFullName() + CommonConstants.DEFAULT_COLON_SPLIT + CommonConstants.AUTH_AUTHENTICATED);
				            LOG.debug(model.getFullName() + CommonConstants.DEFAULT_COLON_SPLIT + CommonConstants.AUTH_AUTHENTICATED);
				            
			            	webRequest.setAttribute(CommonConstants.SESSION_USERID_KEY, user.getUserId());
			            	webRequest.setAttribute(CommonConstants.SESSION_TENANTID_KEY, user.getTenantId());
							webRequest.setAttribute(CommonConstants.SESSION_TOKEN_KEY, tokenManager.getToken(model));
						} else {
				        	LOG.error(input_userinfo + CommonConstants.DEFAULT_COLON_SPLIT + CommonConstants.AUTH_NOT_AUTHENTICATED);
				            //System.out.println(inUsername + CommonConstants.DEFAULT_COLON_SPLIT + CommonConstants.AUTH_NOT_AUTHENTICATED);            
				           	response = Response.status(Response.Status.UNAUTHORIZED).type(MediaType.TEXT_PLAIN).entity(CommonConstants.AUTH_EXCEP_ERR_CRED).build();
				    		throw new WebApplicationException(response);
						}			            
			        } else {
			        	LOG.error(model.getFullName() + CommonConstants.DEFAULT_COLON_SPLIT + CommonConstants.AUTH_NOT_AUTHENTICATED);
			            //System.out.println(model.getFullName() + CommonConstants.DEFAULT_COLON_SPLIT + CommonConstants.AUTH_NOT_AUTHENTICATED);            
			           	response = Response.status(Response.Status.UNAUTHORIZED).type(MediaType.TEXT_PLAIN).entity(CommonConstants.AUTH_EXCEP_ERR_CRED).build();
			    		throw new WebApplicationException(response);
			        }
				} else {
					Object tenantObject = null;
					Object usernameObject = null;
					Object tenantIdObject = null;
					Object userIdObject = null;
					Object tokenObject = null;
			    	HttpSession session = webRequest.getSession(false);
			    	if (session != null) {
			    		tenantObject = session.getAttribute(CommonConstants.SESSION_TENANT_KEY);
			    		usernameObject = session.getAttribute(CommonConstants.SESSION_USERNAME_KEY);
			    		tenantIdObject = session.getAttribute(CommonConstants.SESSION_TENANTID_KEY);
			    		userIdObject = session.getAttribute(CommonConstants.SESSION_USERID_KEY);
			    		tokenObject = session.getAttribute(CommonConstants.SESSION_TOKEN_KEY);
					}
			    	if (tenantObject == null || usernameObject == null || tenantIdObject == null || userIdObject == null || tokenObject == null) {
			    		LOG.error(CommonConstants.AUTH_EXCEP_CRED_REQUIRED);
			    		response = Response.status(Response.Status.UNAUTHORIZED).type(MediaType.TEXT_PLAIN).entity(CommonConstants.AUTH_EXCEP_CRED_REQUIRED).build();
			    		throw new WebApplicationException(response);
			    	} 	    	
			    	else {			        
			            LOG.debug(new StringBuilder().append("User:").append(usernameObject.toString()).append(",Tenant:").append(tenantObject.toString())
			            		.append(CommonConstants.DEFAULT_COLON_SPLIT).append(CommonConstants.AUTH_AUTHENTICATED).toString());
			            webRequest.setAttribute(CommonConstants.SESSION_TENANT_KEY, tenantObject.toString());
			            webRequest.setAttribute(CommonConstants.SESSION_USERNAME_KEY, usernameObject.toString());		            
			            webRequest.setAttribute(CommonConstants.SESSION_TENANTID_KEY, tenantIdObject.toString());
			            webRequest.setAttribute(CommonConstants.SESSION_USERID_KEY, userIdObject.toString());	
			            webRequest.setAttribute(CommonConstants.SESSION_TOKEN_KEY, tokenObject.toString());	

			    		return request;
					}	    
				}
			}    		
       	}
    	
        return request;
    }
}
