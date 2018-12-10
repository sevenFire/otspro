package com.baosight.xinsight.ots.rest.util;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexUtil {
	//只能包含拉丁字母或数字或下划线“_”，只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。
	private final static String tablename_regex = "((?!_{2,})[A-Za-z](([A-Z0-9a-z])*(_)?([A-Z0-9a-z])+)*)";
	
	private final static String basic_regex = "[A-Za-z0-9_]+";
	
	public static boolean isValidTableName(String tablename) {		
		Pattern pattern = Pattern.compile(basic_regex);
		Matcher m = pattern.matcher(tablename);
		boolean match = m.matches();
		if (!match) {
			return false;
		}
		
		pattern = Pattern.compile(tablename_regex);
		m = pattern.matcher(tablename);
		return m.matches();
	}
		
	public static boolean isValidColumnName(String column) {
		return isValidTableName(column);
	}
	
	public static boolean isValidIndexName(String indexname) {
		return isValidTableName(indexname);
	}
	
	public static boolean isValidTenantName(String tenant) {
		return isValidTableName(tenant);
	}

	public static boolean isValidUserName(String username) {
		return isValidTableName(username);
	}
	
	public static void main(String[] args) throws IOException {

		String regex = "((?!_{2,})[A-Za-z](([A-Z0-9a-z])*(_)?([A-Z0-9a-z])+)*)";
		Pattern pattern = Pattern.compile(regex);
		String in = "_a__b_";
		Matcher m = pattern.matcher(in);
		boolean ok = m.matches();
		if (ok != false) {
			System.err.println("error");
		}

		in = "_a__b_";
		m = pattern.matcher(in);
		ok = m.matches();
		if (ok != false) {
			System.err.println("error");
		}
		in = "_a___b_";
		m = pattern.matcher(in);
		ok = m.matches();
		if (ok != false) {
			System.err.println("error");
		}
		in = "_a____b_";
		m = pattern.matcher(in);
		ok = m.matches();
		if (ok != false) {
			System.err.println("error");
		}
		in = "__a___b_";
		m = pattern.matcher(in);
		ok = m.matches();
		if (ok != false) {
			System.err.println("error");
		}
		in = "____a___b_";
		m = pattern.matcher(in);
		ok = m.matches();
		if (ok != false) {
			System.err.println("error");
		}
		in = "a__b";
		m = pattern.matcher(in);
		ok = m.matches();
		if (ok != false) {
			System.err.println("error");
		}		
		in = "_a_b";
		m = pattern.matcher(in);
		ok = m.matches();
		if (ok != false) {
			System.err.println("error");
		}		
		in = "@a_b";
		m = pattern.matcher(in);
		ok = m.matches();
		if (ok != false) {
			System.err.println("error");
		}		
		in = "a_b_";
		m = pattern.matcher(in);
		ok = m.matches();
		if (ok != false) {
			System.err.println("error");
		}		
		in = "a_";
		m = pattern.matcher(in);
		ok = m.matches();
		if (ok != false) {
			System.err.println("error");
		}		
		in = "1_";
		m = pattern.matcher(in);
		ok = m.matches();
		if (ok != false) {
			System.err.println("error");
		}				
		in = "1a_b";
		m = pattern.matcher(in);
		ok = m.matches();
		if (ok != false) {
			System.err.println("error");
		}		
		in = "1_b";
		m = pattern.matcher(in);
		ok = m.matches();
		if (ok != false) {
			System.err.println("error");
		}
		
		///////////////
		in = "a_b";
		m = pattern.matcher(in);
		ok = m.matches();
		if (ok != true) {
			System.err.println("error");
		}
	}
}
