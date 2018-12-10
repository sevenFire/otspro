
package com.baosight.xinsight.ots.mapreduce;

public class Dispatcher {

	public static void main(String[] args) throws Exception {
		if(args.length <1)
		{
			System.out.println("Lack args, please enter mapreduce classname!");
		}
		
		Class<?> mapreduceClass = Class.forName(args[0]);
		OtsMapreduce mapreduce = (OtsMapreduce)mapreduceClass.newInstance();

		int skipCount = 0;
		for(int i = 1; i < args.length; ++i)
		{
			if(args[i].startsWith("-D"))
				skipCount++;
		}
		
		String[] newArgs = new String[args.length - 1 - skipCount];
		
		for(int i = skipCount+1,j = 0; i < args.length; ++i,++j)
		{
			newArgs[j] = args[i];
		}
		System.out.println("new Args length:"+ newArgs.length);
		for(int i = 0; i < newArgs.length; ++i)
		{
			System.out.println(newArgs[i]);
		}
		mapreduce.run(newArgs);
	}
}