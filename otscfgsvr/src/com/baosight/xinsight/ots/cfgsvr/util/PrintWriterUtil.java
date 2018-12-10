package com.baosight.xinsight.ots.cfgsvr.util;

import java.io.PrintWriter;

public class PrintWriterUtil {
	
	public static void PrintAndClose(PrintWriter pw, String result){
		pw.write(result);
		pw.flush();
		pw.close();
	}
}
