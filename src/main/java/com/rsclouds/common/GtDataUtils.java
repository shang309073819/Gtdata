package com.rsclouds.common;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.rsclouds.api.GtDataImpl;
import com.rsclouds.hbase.HbaseBase;

public class GtDataUtils {
	public final static Configuration hbaseConfig = HBaseConfiguration.create();
	
	/**
	 * 转换为gt-data的路径格式
	 * before : /new/test/中文/123.tif
	 *  after : /new/test/%E4%B8%AD%E6%96%87//123.tif	
	 * @param path
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static String format2GtPath(String path) throws UnsupportedEncodingException{
		if(path!= null && path.length()>0){
			while(path.endsWith("/") && path.length()>1){
				path = path.substring(0, path.length()-1);
			}
			path = path.replace("//", "/");
			path = GtDataUtils.replaceLast(path, "/", "//");
			if(!path.contains("%")){
				path = TransCoding.UrlEncode(path, "utf-8");
			}
		}
		return path;
	}
	
	public static String format2DisplayPath(String gtPath) throws UnsupportedEncodingException{
		if(gtPath!= null && gtPath.length()>0){
			while(gtPath.endsWith("/") && gtPath.length()>1){
				gtPath = gtPath.substring(0, gtPath.length()-1);
			}
			gtPath = gtPath.replace("//", "/");
			if(gtPath.contains("%")){
				gtPath = TransCoding.decode(gtPath, "utf-8");
			}
		}
		return gtPath;
	}
	
	
	
	public static String replaceLast(String string, String toReplace,
			String replacement) {
		int pos = string.lastIndexOf(toReplace);
		if (pos > -1) {
			return string.substring(0, pos)
					+ replacement
					+ string.substring(pos + toReplace.length(),
							string.length());
		} else {
			return string;
		}
	}
	 
	/**
	 * 获取gt-data文件大小
	 * @param gtPath
	 * @return
	 * @throws IOException
	 */
	public static String getFileSzie(String gtPath) throws IOException{
		Result rs = HbaseBase.selectRow(GtDataConfig.META_TABLENAME, gtPath);
		if(rs!= null){
			byte[] sizeByte = rs.getValue(GtDataConfig.META_FAMILY, GtDataConfig.META_SIZE);
			if(sizeByte != null && sizeByte.length>0){
				return Bytes.toString(sizeByte);
			}
		}
		return null;
	}

	/**
	 * 自动生成目录结构文件夹
	 * @param path  such as : /map/new/132
	 * @return
	 * @throws IOException
	 */
	public static boolean genterGtdataDir(String path) throws IOException{
		path = path.replace("//", "/");
		while(path.length() > 1 && path.endsWith("/")){
			path = path.substring(0 , path.length() -1);
		}
		String checkPath = replaceLast(path, "/", "//");
		String encodedPath = TransCoding.UrlEncode(checkPath, "utf-8");
		String size = getFileSzie(encodedPath);
		if(size == null ){		
			GtDataImpl.mkdir(encodedPath);
			RedisUtils.redisCheck(encodedPath, GtDataConfig.REDIS_HOST);
			if(checkPath.lastIndexOf("//") > 0){
				String nextPath = checkPath.substring(0,checkPath.lastIndexOf("//"));
				return genterGtdataDir(nextPath);
			}else{
				return true;
			}			
		}else if(size.equals("-1")){
			return true;
		}
		return false;		
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
//		System.out.println("/sss/ffff//ggggg".substring(0, "/sss/ffff//ggggg".lastIndexOf("//")));
//		genterGtdataDir("/users/nanlin//中文");
		String path = "/new/test/中文/123.tif";
		System.out.println(path);
		System.out.println(format2GtPath(path)); 
	}

}
