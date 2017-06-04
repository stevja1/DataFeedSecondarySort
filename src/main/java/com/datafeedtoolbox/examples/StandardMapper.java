package com.datafeedtoolbox.examples;

import com.datafeedtoolbox.examples.tools.DataFeedTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.*;

public class StandardMapper extends Mapper<Object, Text, Text, Text> {
	private static final Logger LOGGER = LoggerFactory.getLogger(StandardMapper.class);
	private static final String FIELD_SEPARATOR = "\\t";
	private static long recordsProcessed = 0;
	private static long lastStatusUpdate = 0;
	public enum MapperCounters {
		CORRUPT_ROW, COLUMN_COUNT, INPUT_COLUMN_COUNT
	}

	private List<String> columnHeaders;
	private Configuration conf;
	private final Text key = new Text();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		URI[] columnHeadersFiles = Job.getInstance(conf).getCacheFiles();
		if(columnHeadersFiles != null && columnHeadersFiles.length > 0) {
			for (URI columnHeadersFile : columnHeadersFiles) {
				Path patternsPath = new Path(columnHeadersFile.getPath());
				String columnHeadersFileName = patternsPath.getName();
				StandardMapper.LOGGER.info("Reading config file: {}", columnHeadersFileName);
				try {
					this.columnHeaders = DataFeedTools.readColumnHeaders(columnHeadersFileName);
					context.getCounter(MapperCounters.COLUMN_COUNT).setValue(this.columnHeaders.size());
				} catch(ParseException e) {
					StandardMapper.LOGGER.error("There was a problem parsing the column headers!");
				}
			}
		}
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		final String[] columns = value.toString().split(StandardMapper.FIELD_SEPARATOR, -1);
		if(columns.length != this.columnHeaders.size()) {
			context.getCounter(MapperCounters.INPUT_COLUMN_COUNT).setValue(columns.length);
			context.getCounter(MapperCounters.CORRUPT_ROW).increment(1);
			return;
		}
		final String visIdHigh = DataFeedTools.getValue("post_visid_high", columns, this.columnHeaders);
		final String visIdLow = DataFeedTools.getValue("post_visid_low", columns, this.columnHeaders);
		this.key.set(String.format("%s:%s", visIdHigh, visIdLow));
		context.write(this.key, value);
		++StandardMapper.recordsProcessed;
		if(System.currentTimeMillis() - StandardMapper.lastStatusUpdate > 5000) {
			final int duration = (int)((System.currentTimeMillis() - StandardMapper.lastStatusUpdate) / 1000);
			StandardMapper.lastStatusUpdate = System.currentTimeMillis();
			context.setStatus("Processed "+StandardMapper.recordsProcessed+" per "+duration+" seconds.");
			LOGGER.info("Processed "+StandardMapper.recordsProcessed+" per "+duration+" seconds.");
			StandardMapper.recordsProcessed = 0;
		}
	}

	public static boolean sampleHit(String postVisidHigh, String postVisidLow, double sampleRate) throws NoSuchAlgorithmException {
		// Convert a rate from something like 4.5% to 450 for use later
		int rate = (int)(sampleRate * 100);
		// Convert the visitor ID to a hash
		MessageDigest digest = MessageDigest.getInstance("SHA-1");
		byte[] hash = digest.digest(String.format("%s:%s", postVisidHigh, postVisidLow).getBytes());

		// If the integer value (returned from Java's .hashCode), modded by 10,000, is less than the
		// rate we calculated above, then include the hit.
		if((hash.hashCode() % 10000) < rate) {
			return true;
		}
		return false;
	}
}