package com.baosight.xinsight.ots.rest.database;

import com.alibaba.fastjson.JSONArray;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DBUtil {

	/**
	 * 带参数的查询
	 * @param conn
	 * @param pstmt
	 * @param paramLst
	 * @return
	 */
	public static ResultSet executeQuery(Connection conn,PreparedStatement pstmt, List paramLst) {
		try{
			for (int i = 0; i < paramLst.size(); i++) {
				Object param = paramLst.get(i);
				pstmt.setObject(i+1, param);
			}
			ResultSet result = pstmt.executeQuery();
			return result;
		}catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 带参数的查询
	 * @param conn
	 * @param pstmt
	 * @param paramLst
	 * @return
	 */
	public static ResultSet executeInsert(Connection conn,PreparedStatement pstmt, List paramLst) {
		try{
			for (int i = 0; i < paramLst.size(); i++) {
				Object param = paramLst.get(i);
				pstmt.setObject(i+1, param);
			}
			ResultSet result = pstmt.executeQuery();
			return result;
		}catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 带参数的更新或插入
	 * @param conn
	 * @param pstmt
	 * @param paramLst
	 * @return
	 */
	public static int executeUpdate(Connection conn, PreparedStatement pstmt,List paramLst){
		try {
			for (int i = 0; i < paramLst.size(); i++) {
				Object param = paramLst.get(i);
				pstmt.setObject(i + 1, param);
			}
			int result = pstmt.executeUpdate();
			return result;
		} catch (SQLException e) {
			if(e.toString().contains("duplicate key value violates unique constraint")){//这条记录是需要被更新的
				return -2; 
			}else{
				e.printStackTrace();
				return -1;
			}
		}	
	}
	

}
