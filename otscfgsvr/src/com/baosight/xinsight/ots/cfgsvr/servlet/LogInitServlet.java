package com.baosight.xinsight.ots.cfgsvr.servlet;

import com.baosight.xinsight.ots.cfgsvr.util.ConfigUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class LogInitServlet extends HttpServlet {  
  
    /**
	 * init system environment
	 */
	private static final long serialVersionUID = 4773210746507169294L;

	@Override
	public void init() throws ServletException {
		
		ConfigUtil.getInstance()._isDebugModeBoolean = Boolean.parseBoolean(this.getInitParameter("Local_Debug_Mode"));
		System.err.println("**" + ConfigUtil.getProjectName() + " Running Mode**:" + (ConfigUtil.getInstance()._isDebugModeBoolean ? "local debug":"running"));
    }
	
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		request.setCharacterEncoding("utf-8"); // 设置编码
		response.setCharacterEncoding("utf-8");
		response.setContentType("text/html;charset=UTF-8");

		try {
			//HttpSession session = request.getSession(false);
			//String tenant = session.getAttribute(Constants.SESSION_TENANT_KEY).toString();
			HashMap<String, String> mapConfig = ConfigUtil.getInstance().getVisibleValues();
			
			Collection<String> keyset = mapConfig.keySet();  
			List<String> listKeys = new ArrayList<String>(keyset);  
			       
			//对key键值按字典升序排序  
			Collections.sort(listKeys);
			StringBuffer configbuffer = new StringBuffer();
			for (int i = 0; i < listKeys.size(); i++) {  
				configbuffer.append(listKeys.get(i)).append("=").append(mapConfig.get(listKeys.get(i))).append("##");  
			}   
			
			PrintWriter writer = response.getWriter();
			writer.print("0," + configbuffer.toString());

			writer.close();
		} catch (Exception e) {
			e.getMessage();

			PrintWriter writer = response.getWriter();
			writer.print("1,获取配置失败！" + e.toString());

			writer.close();
		}
	}
	
	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		this.doGet(request, response);
	}
}
