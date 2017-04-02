package com.rsclouds.entity;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.rsclouds.common.Config;
import com.rsclouds.common.GtDataConfig;

public class Metadata {

	private String rowKey;
	private String url;
	private String dfs;
	private String size;
	private String time;

	public Metadata(String rowKey, String url, String dfs, String size,
			String time) {
		this.url = url;
		this.dfs = dfs;
		this.size = size;
		this.time = time;
		this.rowKey = rowKey;
	}

	public Metadata(Result rs) {
		if (rs != null && !rs.isEmpty()) {
			rowKey = Bytes.toString(rs.getRow());
			url = Bytes.toString(rs.getValue(GtDataConfig.META_FAMILY,
					GtDataConfig.META_URL));
			dfs = Bytes.toString(rs.getValue(GtDataConfig.META_FAMILY,
					GtDataConfig.META_DFS));
			size = Bytes.toString(rs.getValue(GtDataConfig.META_FAMILY,
					GtDataConfig.META_SIZE));
			time = Bytes.toString(rs.getValue(GtDataConfig.META_FAMILY,
					GtDataConfig.META_TIME));
			if ("null".equals(url)) {
				url = null;
			}
			if ("null".equals(dfs)) {
				dfs = null;
			}
			if ("null".equals(size)) {
				size = null;
			}
			if ("null".equals(time)) {
				time = null;
			}
		}
	}

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDfs() {
		return dfs;
	}

	public void setDfs(String dfs) {
		this.dfs = dfs;
	}

	public String getSize() {
		return size;
	}

	public void setSize(String size) {
		this.size = size;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public Map<String, String> toStrMap() {
		Map<String, String> map = new HashMap<String, String>();
		map.put(Config.METADATA_ATTS_SIZE, size);
		map.put(Config.METADATA_ATTS_URL, url);
		map.put(Config.METADATA_ATTS_DFS, dfs);
		map.put(Config.METADATA_ATTS_TIME, time);
		return map;
	}
}
