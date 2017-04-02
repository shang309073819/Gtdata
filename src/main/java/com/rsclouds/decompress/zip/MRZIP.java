package com.rsclouds.decompress.zip;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rsclouds.common.MD5Calculate;

public class MRZIP extends Configured implements Tool {
	private static final Logger LOG = LoggerFactory.getLogger(MRZIP.class);

	static class ZipDecompressMapper extends
			Mapper<Text, BytesWritable, Text, Text> {

		@Override
		protected void map(Text key, BytesWritable value,
				Mapper<Text, BytesWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {

			byte[] md5Bytes = null;
			String md5Str = "";
			if (key.toString().endsWith("/")) {
			} else if (value.getLength() == 0) {
				// 空文件使用一样的MD5
				md5Str = "d41d8cd98f00b204e9800998ecf8427e";
				md5Bytes = Bytes.toBytes(md5Str);
			} else {
				md5Str = MD5Calculate.fileByteMD5(value.copyBytes());
				md5Bytes = Bytes.toBytes(md5Str);
			}

			if (md5Bytes == null) {
			} else {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				FSDataOutputStream out = fs.create(new Path("/tmp/tar/ext"
						+ new String(md5Bytes)));
				out.write(value.copyBytes(), 0, value.getLength());
				out.close();
				fs.close();
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = (getConf() == null ? new Configuration()
				: getConf());

		Job job = Job.getInstance(conf);

		job.setJarByClass(MRZIP.class);
		job.setMapperClass(MRZIP.ZipDecompressMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(ZipInputFormat.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path("/tmp/tar/in"));
		FileOutputFormat.setOutputPath(job, new Path("/tmp/tar/out"));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		;
		int exitCode = ToolRunner.run(new MRZIP(), args);
		long end = System.currentTimeMillis();
		LOG.info("ZIP decompress time : " + (end - start) + "(ms)");
		System.exit(exitCode);
	}
}
