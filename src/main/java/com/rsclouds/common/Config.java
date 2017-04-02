package com.rsclouds.common;

import org.apache.hadoop.hbase.util.Bytes;


/**
 * gt-parallel整体配置文件
 * @author wugq
 *
 */
public class Config {
	
	/*gt-data*/
	public static String METADATA_TABLE = ConfProperty.getInstance().getStringValue("meta.table");
	public static String RESOURCE_TABLE = ConfProperty.getInstance().getStringValue("resource.table");
	public static final String METADATA_ATTS = "atts";
	public static final String METADATA_ATTS_URL = "url";
	public static final String METADATA_ATTS_DFS = "dfs";
	public static final String METADATA_ATTS_SIZE = "size";
	public static final String METADATA_ATTS_TIME = "time";
	public static final String RESOURCE_IMG = "img";
	public static final String RESOURCE_IMG_DATA = "data";
	public static final String RESOURCE_IMG_LINKS = "links";
	public static final String HBASE_SEPARATOR = "/";
	
	/*cutting*/
	public static final String CUTTING_OUTPUTPATH = "cutting_outputPath";
	public static final String TRASH = "/Trash";
	public static final String ROWKEY = "rowkey";
	public static final String CUTTING_INPUTFILE = "cutting_input_path";
	public static int WIDTH_DEFAULT = ConfProperty.getInstance().getIntValue("cutting.width.rang");
	public static int HEIGHT_DEFAULT = ConfProperty.getInstance().getIntValue("cutting.height.rang");
	public static int IMGBLOCK_WIDTH = ConfProperty.getInstance().getIntValue("cutting.imgblock.rang");
	 
	
	/*job_meta*/
	public static String JOB_TABLE = ConfProperty.getInstance().getStringValue("job.table");
	public static final String JOB_META = "meta";
	public static final String JOB_META_PID = "pid";
	public static final String JOB_META_IN_PATH = "in_path";
	public static final String JOB_META_OUT_PATH = "out_path";
	public static final String JOB_META_NODE = "node";
	public static final String JOB_META_STATE = "state";
	public static final String JOB_META_TYPE = "type";
	public static final String JOB_META_PROGRESS = "progress";//进度
	public static final String JOB_META_PART = "part";
	public static final String JOB_META_JID = "jid";//记录任务在hadoop中执行的job号
	public static final String JOB_META_START_TIME = "start_time";
	public static final String JOB_META_END_TIME = "end_time";
	
	public static final byte[] JOB_BYTE_META = Bytes.toBytes(Config.JOB_META);
	public static final byte[] JOB_BYTE_META_PID = Bytes.toBytes(Config.JOB_META_PID);
	public static final byte[] JOB_BYTE_META_IN_PATH = Bytes.toBytes(Config.JOB_META_IN_PATH);
	public static final byte[] JOB_BYTE_META_OUT_PATH = Bytes.toBytes(Config.JOB_META_OUT_PATH);
	public static final byte[] JOB_BYTE_META_NODE = Bytes.toBytes(Config.JOB_META_NODE);
	public static final byte[] JOB_BYTE_META_STATE = Bytes.toBytes(Config.JOB_META_STATE);
	public static final byte[] JOB_BYTE_META_TYPE = Bytes.toBytes(Config.JOB_META_TYPE);
	public static final byte[] JOB_BYTE_META_PROGRESS = Bytes.toBytes(Config.JOB_META_PROGRESS);
	public static final byte[] JOB_BYTE_META_PART = Bytes.toBytes(Config.JOB_META_PART);
	public static final byte[] JOB_BYTE_META_JID = Bytes.toBytes(Config.JOB_META_JID);
	public static final byte[] JOB_BYTE_META_START_TIME = Bytes.toBytes(Config.JOB_META_START_TIME);
	public static final byte[] JOB_BYTE_META_END_TIME = Bytes.toBytes(Config.JOB_META_END_TIME);
	
	public static final String[] LEVEL = {"_allLayers","L00" ,"L01" ,"L02","L03","L04","L05","L06","L07","L08","L09","L0a","L0b","L0c","L0d","L0e","L0f","L10","L11","L12","L13","L14"};

	
	
	public enum JOB_STATE{
		ACCEPTED,
		RUNNING,
		FAILED,
		SUCCEEDED,
		PAUSED,
		CANCELLED
	}
	
	public enum DECOMPRESS_TYPE{
		NO,
		ZIP,
		RAR,
		GZ,
	}
	
	public enum JOB_TYPE{
		IMPORT,
		EXPORT,
		CUTTING,
		MKDIR,
		RENAME,
		DELETE,
		SEARCH,
		LIST,
	}
	
	public static final String JOB_OP_LOCAL_IMPORT = "LOCAL_IMPORT";
	public static final String JOB_OP_LOCAL_ZIP_IMPORT = "LOCAL_ZIP_IMPORT";
	public static final String JOB_OP_EXPORT = "EXPORT";
	public static final String JOB_OP_CUTTING = "CUTTING";
	public static final String JOB_OP_MKDIR = "MKDIR";
	public static final String JOB_OP_RENAME = "RENAME";
	public static final String JOB_OP_DELETE = "DELETE";
	public static final String JOB_OP_SEARCH = "SEARCH";
	public static final String JOB_OP_LIST = "LIST";
	
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(DECOMPRESS_TYPE.NO.name().equals("NO"));
	}

}
