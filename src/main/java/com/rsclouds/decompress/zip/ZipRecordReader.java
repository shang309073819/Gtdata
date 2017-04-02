package com.rsclouds.decompress.zip;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.IOUtils;

public class ZipRecordReader extends RecordReader<Text, BytesWritable> {
	private static final Logger LOG = LoggerFactory
			.getLogger(ZipRecordReader.class);
	private Configuration conf;
	private BytesWritable value = new BytesWritable();
	private Text key = new Text();
	private boolean processed = false;
	private long readLength;
	private long length;
	private ZipInputStream zipInputStream;
	FSDataInputStream in = null;

	public Text createKey() {
		return new Text();
	}

	public BytesWritable createValue() {
		return new BytesWritable();
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		float pro;
		if (readLength >= length) {
			pro = 0.99f;
		} else {
			pro = (float) readLength / length;
		}
		return processed ? 1.0f : pro;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit filesplit = (FileSplit) split;
		this.conf = context.getConfiguration();
		this.readLength = 0;
		this.length = filesplit.getLength();
		Path filePath = filesplit.getPath();

		try {
			FileSystem fs = filePath.getFileSystem(conf);
			in = fs.open(filePath);
			zipInputStream = new ZipInputStream(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		ZipEntry zipEntry = zipInputStream.getNextEntry();
		if (zipEntry != null) {
			byte[] contents = new byte[(int) zipEntry.getSize()];
			LOG.info("contents.length = " + contents.length
					+ ",CompressedSize = " + zipEntry.getCompressedSize());
			contents = IOUtils
					.readFully(zipInputStream, contents.length, false);
			String keyStr = zipEntry.getName();
			if (zipEntry.isDirectory()) {
				if (!keyStr.endsWith("/")) {
					keyStr += "/";
				}
			}
			key.set(keyStr);
			value.set(contents, 0, contents.length);
			readLength += zipEntry.getCompressedSize();
			LOG.info("nextKeyValue:readLength=" + readLength + ",length="
					+ length);
			return true;
		} else {
			processed = true;
			if (in != null)
				in.close();
			if (zipInputStream != null)
				zipInputStream.close();
			LOG.info("end nextKeyValue:readLength=" + readLength + ",length="
					+ length);
		}
		return false;
	}

}
