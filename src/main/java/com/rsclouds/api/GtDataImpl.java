package com.rsclouds.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.rsclouds.common.Config;
import com.rsclouds.common.GtDataConfig;
import com.rsclouds.common.GtDataUtils;
import com.rsclouds.common.TransCoding;
import com.rsclouds.entity.Metadata;
import com.rsclouds.hbase.HbaseBase;

public class GtDataImpl {

	public static List<Map<String, String>> list(String gtPath)
			throws IOException {
		List<Map<String, String>> fileMapList = new ArrayList<Map<String, String>>();
		try {
			gtPath = GtDataUtils.format2GtPath(gtPath).replace("//", "/");
			List<Result> results = HbaseBase.Scan(Config.METADATA_TABLE, 0,
					gtPath + "//", gtPath + "//{", null, null);
			for (Result rs : results) {
				Map<String, String> fileMap = new HashMap<String, String>();
				String sizeStr = Bytes.toString(rs.getValue(
						GtDataConfig.META_FAMILY, GtDataConfig.META_SIZE));
				String timeStr = Bytes.toString(rs.getValue(
						GtDataConfig.META_FAMILY, GtDataConfig.META_TIME));
				String rowKey = Bytes.toString(rs.getRow());
				if (rowKey.contains("%")) {
					rowKey = TransCoding.decode(rowKey, "utf-8");
				}
				fileMap.put("path", rowKey.replace("//", "/"));
				fileMap.put("size", sizeStr);
				fileMap.put("time", timeStr);
				fileMapList.add(fileMap);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		return fileMapList;
	}

	public static boolean mkdir(String gtPath) throws IOException {
		try {
			gtPath = GtDataUtils.format2GtPath(gtPath);
			Map<String, String> map = new HashMap<String, String>();
			map.put(Config.METADATA_ATTS_SIZE, "-1");
			map.put(Config.METADATA_ATTS_URL, "");
			map.put(Config.METADATA_ATTS_DFS, "0");
			map.put(Config.METADATA_ATTS_TIME, "" + System.currentTimeMillis());
			HbaseBase.writeRows(Config.METADATA_TABLE, gtPath,
					Config.METADATA_ATTS, map);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public static List<Metadata> search(String gtPath) throws IOException {
		List<Metadata> metas = new ArrayList<Metadata>();
		try {
			gtPath = GtDataUtils.format2GtPath(gtPath);
			// ....TODO
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		return metas;
	}

	public static boolean export(String gtpath, OutputStream out)
			throws IOException {
		try {
			gtpath = GtDataUtils.format2GtPath(gtpath);
			Result result = HbaseBase.selectRow(Config.METADATA_TABLE, gtpath);
			if (result != null && !result.isEmpty()) {
				String dfsStr = new String(result.getValue(
						GtDataConfig.META_FAMILY, GtDataConfig.META_DFS));
				int dfsInt = Integer.parseInt(dfsStr);
				byte[] md5Bytes = result.getValue(GtDataConfig.META_FAMILY,
						GtDataConfig.META_URL);
				String md5Str = Bytes.toString(md5Bytes);
				if (dfsInt == 0) {
					Result resultRes = HbaseBase.selectRow(
							Config.RESOURCE_TABLE, md5Str);
					;
					if (resultRes == null) {
						out.close();
						return false;
					} else {
						byte[] value = resultRes.getValue(
								GtDataConfig.RESOURCE_FAMILY,
								GtDataConfig.RESOURCE_DATA);
						out.write(value, 0, value.length);
						out.flush();
						out.close();
					}
				} else if (dfsInt == 1) {
					FileSystem fs = FileSystem.get(HbaseBase.getHbaseConf());
					FSDataInputStream in = fs.open(new Path(
							GtDataConfig.HDFSHOST_PATH + md5Str));
					int readLen;
					byte[] value = new byte[1024];
					while ((readLen = in.read(value, 0, 1024)) != -1) {
						out.write(value, 0, readLen);
					}
					in.close();
				} else {
					out.close();
					return false;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		return true;
	}

	public static boolean importStream(String outPath, InputStream in,
			String md5) throws IOException {
		try {
			outPath = GtDataUtils.format2GtPath(outPath);
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		return false;
	}

	/**
	 * @param args
	 * @throws UnsupportedEncodingException
	 */
	public static void main(String[] args) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		System.out.println(TransCoding.UrlEncode("/123/中文", "utf-8"));
		System.out.println(TransCoding.UrlEncode(
				TransCoding.UrlEncode("/123/中文", "utf-8"), "utf-8"));
	}

}
