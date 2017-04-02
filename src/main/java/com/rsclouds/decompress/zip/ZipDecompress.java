package com.rsclouds.decompress.zip;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rsclouds.common.GtDataConfig;
import com.rsclouds.common.GtDataUtils;
import com.rsclouds.common.MD5Calculate;
import com.rsclouds.common.TransCoding;

public class ZipDecompress extends Configured implements Tool {
	private static final Logger LOG = LoggerFactory
			.getLogger(ZipDecompress.class);

	static class ZipDecompressMapper extends
			Mapper<Text, BytesWritable, Text, Text> {
		final String ONE = "1";
		final String NEGATIVE_ONE = "-1";
		final String ZERO = "0";
		String storagePath = "storage_path";

		Configuration conf;
		Configuration hbaseConfig;
		HTable resTable = null;
		StringBuilder filePathBuild = new StringBuilder("");
		Text keyOut = new Text();
		Text valueOut = new Text();

		public void setup(Context context) throws IOException,
				InterruptedException {
			conf = context.getConfiguration();
			storagePath = conf.get("storage_path");
			hbaseConfig = HBaseConfiguration.create();
			resTable = new HTable(hbaseConfig, GtDataConfig.RESOURCE_TABLENAME);
			while (storagePath.length() > 1 && storagePath.endsWith("/")) {
				storagePath = storagePath
						.substring(0, storagePath.length() - 1);
			}
		}

		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			try {

				filePathBuild.append(storagePath + "/" + key.toString());

				if (filePathBuild.lastIndexOf("/") == filePathBuild.length() - 1) {
					filePathBuild.deleteCharAt(filePathBuild.length() - 1);
				}

				filePathBuild.replace(0, filePathBuild.length(), TransCoding
						.UrlEncode(filePathBuild.toString(), "utf-8"));
				filePathBuild.replace(0, filePathBuild.length(), GtDataUtils
						.replaceLast(filePathBuild.toString(), "/", "//"));

				byte[] md5Bytes = null;
				String md5Str = "";
				long fileSize = value.getLength();
				if (key.toString().endsWith("/")) {
					// 写入文件夹
				} else if (value.getLength() == 0) {
					// 空文件使用一样的MD5
					md5Str = "d41d8cd98f00b204e9800998ecf8427e";
					md5Bytes = Bytes.toBytes(md5Str);
				} else {
					md5Str = MD5Calculate.fileByteMD5(value.copyBytes());
					md5Bytes = Bytes.toBytes(md5Str);
				}
				String url = "";
				if (md5Bytes == null) {
					fileSize = -1;
					url = "";
				} else {
					url = md5Str;

					Put resPut = new Put(md5Bytes);
					Get get = new Get(md5Bytes);
					Result result = resTable.get(get);
					if (result != null && !result.isEmpty()) {
					} else {// file doesn't exists on gt-data
						// LOG.info("======file doesn't exists on gt-data:"+md5Str);
						resPut.add(GtDataConfig.RESOURCE_FAMILY,
								GtDataConfig.RESOURCE_LINKS, GtDataConfig.ONE);
						if (fileSize < 16777216) {// less than 16MB,input hbase
							resPut.add(GtDataConfig.RESOURCE_FAMILY,
									GtDataConfig.RESOURCE_DATA,
									value.copyBytes());
						} else {// more than 16MB, input hdfs
							FileSystem fs = FileSystem.get(hbaseConfig);
							FSDataOutputStream out = fs.create(new Path(
									GtDataConfig.HDFSHOST_PATH
											+ new String(md5Bytes)));
							out.write(value.copyBytes(), 0, value.getLength());
							out.close();
							fs.close();
						}
						resTable.put(resPut);
					}
				}
				keyOut.set(filePathBuild.toString());
				if (fileSize < 16777216) {
					valueOut.set(fileSize + "," + url + "," + ZERO);
				} else {
					valueOut.set(fileSize + "," + url + "," + ONE);
				}
				context.write(keyOut, valueOut);
			} catch (IOException e) {
				e.printStackTrace();
				throw e;
			} finally {
				filePathBuild.delete(0, filePathBuild.length());
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			if (resTable != null) {
				resTable.flushCommits();
				resTable.close();
			}
		}
	}

	static class ZipDecompressReducer extends
			TableReducer<Text, Text, NullWritable> {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable metaTable = null;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if (metaTable == null) {
				metaTable = new HTable(hbaseConfig, GtDataConfig.META_TABLENAME);
			}
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {
				Text value = it.next();
				LOG.info(key.toString() + " : " + value.toString());
				String[] args = value.toString().split(",");
				if (args.length != 3) {
					// error
				}
				byte[] rowkey = Bytes.toBytes(key.toString());
				Get get = new Get(rowkey);
				Result result = metaTable.get(get);
				if (result == null || result.isEmpty()) {
					Put metaPut = new Put(rowkey);
					metaPut.add(GtDataConfig.META_FAMILY,
							GtDataConfig.META_SIZE, Bytes.toBytes(args[0]));
					metaPut.add(GtDataConfig.META_FAMILY,
							GtDataConfig.META_URL, Bytes.toBytes(args[1]));
					metaPut.add(GtDataConfig.META_FAMILY,
							GtDataConfig.META_DFS, Bytes.toBytes(args[2]));
					metaPut.add(GtDataConfig.META_FAMILY,
							GtDataConfig.META_TIME,
							Bytes.toBytes("" + System.currentTimeMillis()));
					context.write(NullWritable.get(), metaPut);
				}
			}
		}
	}

	/**
	 * 创建表
	 */
	public static boolean createTable(String tablename, String[] cfs)
			throws IOException {
		Configuration conf = HBaseConfiguration.create();
		@SuppressWarnings("resource")
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			System.out.println("table is already exist : " + tablename);
			return false;
		} else {
			TableName tm = TableName.valueOf(tablename);
			HTableDescriptor tableDesc = new HTableDescriptor(tm);
			for (int i = 0; i < cfs.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(cfs[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("createTable success : " + tablename);
			return true;
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			LOG.info("usage: <inputpath> <Storage_path>");
			return 0;
		}
		Configuration conf = getConf() == null ? HBaseConfiguration.create()
				: getConf();
		conf.set("mapred.child.java.opts", "-Xmx4096m");
		conf.setInt("mapred.tasktracker.map.tasks.maximum", 1);

		conf.set("storage_path", args[1]);
		conf.set(TableOutputFormat.OUTPUT_TABLE, GtDataConfig.META_TABLENAME);
		Job job = Job.getInstance(conf);

		createTable(GtDataConfig.META_TABLENAME,
				new String[] { Bytes.toString(GtDataConfig.META_FAMILY) });
		createTable(GtDataConfig.RESOURCE_TABLENAME,
				new String[] { Bytes.toString(GtDataConfig.RESOURCE_FAMILY) });

		job.setJarByClass(ZipDecompress.class);
		job.setMapperClass(ZipDecompressMapper.class);
		job.setReducerClass(ZipDecompressReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(ZipInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setNumReduceTasks(6);

		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fileStatus = fs.listStatus(new Path(args[0]));
		List<Path> paths = new ArrayList<Path>();
		for (int i = 0; i < fileStatus.length; i++) {
			Path p = fileStatus[i].getPath();
			if (p.getName().endsWith(".zip")) {
				paths.add(p);
			}
		}
		FileInputFormat.setInputPaths(job,
				paths.toArray(new Path[paths.size()]));

		// 生成gtdata输出目录结构
		if (!GtDataUtils.genterGtdataDir(args[1])) {
			LOG.info("ERROR <Storage_path> : " + args[1]);
			return 1;
		}
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		// args = new String[2];
		// args[0] = "hdfs://node03.rsclouds.cn:8020/nanlin_root/L16.zip";
		// args[1] = "/import/test16/_alllayers/";
		int exitCode = ToolRunner.run(new ZipDecompress(), args);
		long end = System.currentTimeMillis();
		LOG.info("ZIP decompress time : " + (end - start) + "(ms)");
		System.exit(exitCode);
	}

}
