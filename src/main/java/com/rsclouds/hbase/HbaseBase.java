package com.rsclouds.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseBase {

	private static Configuration conf = HBaseConfiguration.create();
	//private static HTablePool pool;
	/**
	 * HBaseConfiguration
	 */
	public static Configuration getHbaseConf(){
		return conf;
	}


	/**
	 * createTable
	 * 
	 * @throws IOException
	 */
	public static boolean createTable(String tablename, String[] cfs) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			System.out.println("table is already exist : " + tablename);
			return false;
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tablename);
			for (int i = 0; i < cfs.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(cfs[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("createTable success : " + tablename);
			return true;
		}	
	}

	/**
	 * deleteTable
	 * 
	 * @param tablename
	 * @throws IOException
	 */
	public static boolean deleteTable(String tablename) throws IOException {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tablename);
			admin.deleteTable(tablename);
			System.out.println("deleteTable success : " + tablename);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			throw e;
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			throw e;
		}
		return true;
	}

	/**
	 * write a Row
	 * 
	 * @param tablename
	 * @param cfs
	 * @throws IOException 
	 */
	public static void writeRow(String tablename, String rowkey, String family,String qualifier, String value) throws IOException {
		HTable table = new HTable(conf, tablename);
		Put put = new Put(Bytes.toBytes(rowkey));
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		table.put(put);	
	}
	
	/**
	 * write Rows
	 * 
	 * @param tablename
	 * @param cfs
	 * @throws IOException 
	 */
	public static void writeRows(String tablename, String rowkey, String family,Map<String,String> qv) throws IOException {
		HTable table = new HTable(conf, tablename);
		Put put = new Put(Bytes.toBytes(rowkey));
		for(String qualifier : qv.keySet()){
			String value = qv.get(qualifier);
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		}	
		table.put(put);	
	}

	/**
	 * delete a Row
	 * 
	 * @param tablename
	 * @param rowkey
	 * @throws IOException
	 */
	public static void deleteRow(String tablename, String rowkey) throws IOException {
		HTable table = new HTable(conf, tablename);
		List<Delete> list = new ArrayList<Delete>();
		Delete d1 = new Delete(Bytes.toBytes(rowkey));
		list.add(d1);
		table.delete(list);
		System.out.println("delete a Row success : " + tablename + " : " + rowkey);
	}

	/**
	 * select Row
	 * 
	 * @param tablename
	 * @param rowkey
	 */
	public static Result selectRow(String tablename, String rowKey)
			throws IOException {
		HTable table =new HTable(conf, tablename);
		Get g = new Get(Bytes.toBytes(rowKey));
		// g.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		Result rs = table.get(g);
		// rs.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		table.close();
		return rs;
	}

	/**
	 * 根据rowkey的关键字搜索一条记录
	 * @param tablename
	 * @param keyWord
	 * @return
	 * @throws IOException
	 */
	public static List<Result> selectByRowFilter(String tablename, String keyWord)
			throws IOException {
		List<Result> results = new ArrayList<Result>();
		HTable table = new HTable(conf, tablename);
		Scan scan = new Scan();
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(keyWord));
		//Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(keyWord));//such as : "resource/XTYY/public/map/cia/china_cia/Layers/_alllayers/L../"
		scan.setFilter(filter);
		ResultScanner rs = table.getScanner(scan);
		for (Result res : rs) {
			 results.add(res);
		}
		rs.close();
		table.close();
		return results;
	}

	/**
	 * 根据范围搜索相关记录
	 * @param tablename
	 * @param startRow
	 * @param stopRow
	 * @return
	 * @throws IOException
	 */
	public static List<Result> selectByRegions(String tablename,String startRow,String stopRow) throws IOException {
		List<Result> results = new ArrayList<Result>();
		HTable table = new HTable(conf, tablename);
		Scan scan = new Scan();
		scan.setMaxVersions();
		if(startRow!=null)
			scan.setStartRow(Bytes.toBytes(startRow));
		if(stopRow!=null)
			scan.setStopRow(Bytes.toBytes(stopRow));
		ResultScanner rs = table.getScanner(scan);
		for (Result res : rs) {
			 results.add(res);
		}
		rs.close();
		table.close();
		return results;
	}

	
	public static List<Result> selectByFilter(String tablename, List<String> arr)
			throws IOException {
		List<Result> results = new ArrayList<Result>();
		HTable table=new HTable(conf,tablename);
		FilterList filterList = new FilterList();
		Scan scan = new Scan();
		for (String v : arr) { // 各个条件之间是“与”的关系
			String[] s = v.split(",");
			filterList.addFilter(new SingleColumnValueFilter(Bytes
					.toBytes(s[0]), Bytes.toBytes(s[1]), CompareOp.EQUAL, Bytes
					.toBytes(s[2])));
			// 添加下面这一行后，则只返回指定的cell，同一行中的其他cell不返回
			// s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
		}
		scan.setFilter(filterList);
		ResultScanner rs = table.getScanner(scan);
		for (Result res : rs) {
			 results.add(res);
		}
		rs.close();
		table.close();
		return results;
	}
	

	public static List<Result> Scan(String tablename,int size,String startRow,String stopRow,String keyWord,List<String> arr) throws IOException{
		List<Result> results = new ArrayList<Result>();
		HTable table=new HTable(conf,tablename);
		Scan scan = new Scan();
		scan.setMaxVersions();
		if(size<0)
			size=50;
		if(startRow!=null)
			scan.setStartRow(Bytes.toBytes(startRow));
		if(stopRow!=null)
			scan.setStopRow(Bytes.toBytes(stopRow));
		FilterList filterList = new FilterList();
		if(keyWord != null)
			filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(keyWord)));
		if(arr != null){
			for (String v : arr) { // 各个条件之间是“与”的关系
				String[] s = v.split(",");
				filterList.addFilter(new SingleColumnValueFilter(Bytes
						.toBytes(s[0]), Bytes.toBytes(s[1]), CompareOp.EQUAL, Bytes
						.toBytes(s[2])));
				// 添加下面这一行后，则只返回指定的cell，同一行中的其他cell不返回
				// s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
			}
		}
		if(keyWord!=null || arr != null)
			scan.setFilter(filterList);		
		ResultScanner rs = table.getScanner(scan);
		if(startRow!=null || stopRow == null){
			int count = 0;
			for (Result res : rs) {
				 results.add(res);
				 count++;
				 if(size != 0 && count == size){
					 break;
				 }
			}
		}else if(stopRow!=null){		
			for (Result res : rs) {
				 results.add(res);
			}
			int length = results.size();
			if(size != 0  && length - size > 0 ){
				results.subList(length - size, length-1);
			}
		}
		rs.close();
		table.close();
		return results;		
	}

	/**
	 * 列出所有table
	 * @return
	 * @throws IOException
	 */
	public static List<String> listTable() throws IOException {
		List<String> result = new ArrayList<String>();
		HBaseAdmin admin = new HBaseAdmin(conf);
		for (TableName name : admin.listTableNames()) {
			result.add(name.getNameAsString());
			System.out.println(name.getNameAsString());
		}
		return result;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		listTable();
		//for(Result rs : selectByRegions(HbaseConfig.METADATA_TABLE, "resource/XTYY/public/china_cia/Layers/_allLayers/", "resource/XTYY/public/china_cia/Layers/_allLayers//L02")){
		//for(Result rs : selectByRegions(HbaseConfig.METADATA_TABLE, "nanlin", "nanlin/test")){	
		//	System.out.println(rs);
		//}
		//System.out.println(selectRow(HbaseConfig.METADATA_TABLE,"/nanlin//test"));	
//		System.out.println(Scan(HbaseConfig.METADATA_TABLE,20, null,null, null, null));
//		System.out.println(createTable("nanlin", new String[]{"atts"}));
	}

}
