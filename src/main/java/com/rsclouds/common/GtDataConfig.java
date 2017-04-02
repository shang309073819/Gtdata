package com.rsclouds.common;

import org.apache.hadoop.hbase.util.Bytes;

public class GtDataConfig {

	public static String HDFSHOST_PATH = ConfProperty.getInstance()
			.getStringValue("hdfs.path");
	public static String REDIS_HOST = ConfProperty.getInstance()
			.getStringValue("redis.host");
	public static int REDIS_PORT = ConfProperty.getInstance().getIntValue(
			"redis.port");

	public static String RESOURCE_TABLENAME = ConfProperty.getInstance()
			.getStringValue("resource.table");
	public static final byte[] RESOURCE_FAMILY = Bytes.toBytes("img");
	public static final byte[] RESOURCE_LINKS = Bytes.toBytes("links");
	public static final byte[] RESOURCE_DATA = Bytes.toBytes("data");

	public static String META_TABLENAME = ConfProperty.getInstance()
			.getStringValue("meta.table");
	public static final byte[] META_FAMILY = Bytes.toBytes("atts");
	public static final byte[] META_URL = Bytes.toBytes("url");
	public static final byte[] META_DFS = Bytes.toBytes("dfs");
	public static final byte[] META_SIZE = Bytes.toBytes("size");
	public static final byte[] META_TIME = Bytes.toBytes("time");
	public static final byte[] ONE = Bytes.toBytes("1");
	public static final byte[] ZERO = Bytes.toBytes("0");
	public static final byte[] NEGATIVE_ONE = Bytes.toBytes("-1");

}
