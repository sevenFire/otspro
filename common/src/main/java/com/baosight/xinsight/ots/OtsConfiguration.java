package com.baosight.xinsight.ots;

import java.util.Properties;
import java.util.Set;

public class OtsConfiguration extends Properties {
	private static final long serialVersionUID = 1L;

	public OtsConfiguration() {
		super();
	}

	public OtsConfiguration(final Properties c) {
		this();
		merge(this, c);
	}

	public static void merge(Properties destConf, Properties srcConf) {
		Set<Object> keys = srcConf.keySet();
		for (Object key : keys) {
			destConf.put(key, srcConf.get(key));
		}
	}

	public static OtsConfiguration create() {
		OtsConfiguration conf = new OtsConfiguration();
		return conf;
	}
}
