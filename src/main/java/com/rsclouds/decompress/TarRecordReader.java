package com.rsclouds.decompress;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
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

public class TarRecordReader extends RecordReader<Text, BytesWritable> {
	private static final Logger LOG = LoggerFactory
			.getLogger(TarRecordReader.class);
	private Configuration conf;
	private BytesWritable value = new BytesWritable();
	private Text key = new Text();
	private boolean processed = false;
	private long readLength;
	private long length;
	private FSDataInputStream in = null;
	private TarArchiveInputStream tarIn = null;

	public Text createKey() {
		return new Text();
	}

	public BytesWritable createValue() {
		return new BytesWritable();
	}

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

		// modify by chenshang
		FileSystem fs = filePath.getFileSystem(conf);
		in = fs.open(filePath);
		GZIPInputStream is = new GZIPInputStream(new BufferedInputStream(in));
		try {
			tarIn = (TarArchiveInputStream) new ArchiveStreamFactory()
					.createArchiveInputStream("tar", is);
		} catch (ArchiveException e) {
			e.printStackTrace();
		}

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// modify by chenshang
		BufferedInputStream bufferedInputStream = new BufferedInputStream(tarIn);
		TarArchiveEntry entry = tarIn.getNextTarEntry();
		if (entry != null) {
			ByteArrayOutputStream bos = new ByteArrayOutputStream(
					(int) entry.getSize());
			String keyStr = entry.getName();
			if (entry.isDirectory()) {
				if (!keyStr.endsWith("/")) {
					keyStr += "/";
				}
			}
			try {
				int buf_size = 1024;
				byte[] buffer = new byte[buf_size];
				int len = 0;
				while (-1 != (len = bufferedInputStream.read(buffer, 0,
						buf_size))) {
					bos.write(buffer, 0, len);
				}

				key.set(keyStr);
				value.set(bos.toByteArray(), 0, bos.toByteArray().length);
				readLength += entry.getRealSize();
				return true;
			} catch (IOException e) {
				e.printStackTrace();
				throw e;
			} finally {
				try {
					bufferedInputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				bos.close();
			}
		} else {
			processed = true;
			if (in != null)
				in.close();
			if (tarIn != null)
				tarIn.close();
			LOG.info("end nextKeyValue:readLength=" + readLength + ",length="
					+ length);
		}
		return false;
	}
}
