package com.baosight.xinsight.ots.rest.filter;

import javax.ws.rs.ext.Provider;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;


@Provider
public class CrossDomainFilter implements ContainerResponseFilter {
	 /**
     * Add the cross domain data to the output if needed
     *
     * @param request The container request (input)
     * @param response The container request (output)
     * @return The output request with cross domain if needed
     */
	@Override
	public ContainerResponse filter(ContainerRequest request, ContainerResponse response) {
		response.getHttpHeaders().add("Access-Control-Allow-Origin", "*");
		response.getHttpHeaders().add("Access-Control-Allow-Headers", "origin, content-type, accept, authorization");
		response.getHttpHeaders().add("Access-Control-Allow-Credentials", "true");
		response.getHttpHeaders().add("Access-Control-Allow-Methods", "GET, OPTIONS, HEAD");//"GET, POST, PUT, DELETE, OPTIONS, HEAD"
		response.getHttpHeaders().add("Access-Control-Max-Age", "1209600");
		return response;
	}
}
