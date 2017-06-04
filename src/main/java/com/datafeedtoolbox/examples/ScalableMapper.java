package com.datafeedtoolbox.examples;

import com.datafeedtoolbox.examples.secondarysort.CompositeDataFeedKey;
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
import java.util.List;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class ScalableMapper extends Mapper<Object, Text, CompositeDataFeedKey, Text> {
	private static final Logger LOGGER = LoggerFactory.getLogger(ScalableMapper.class);
	private static final String FIELD_SEPARATOR = "\\t";
	private static long recordsProcessed = 0;
	private static long lastStatusUpdate = 0;
	public enum MapperCounters {
		CORRUPT_ROW, COLUMN_COUNT, INPUT_COLUMN_COUNT
	}

	private List<String> columnHeaders;
	private Configuration conf;
	private final CompositeDataFeedKey key = new CompositeDataFeedKey();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		URI[] columnHeadersFiles = Job.getInstance(conf).getCacheFiles();
		if(columnHeadersFiles != null && columnHeadersFiles.length > 0) {
			for (URI columnHeadersFile : columnHeadersFiles) {
				Path patternsPath = new Path(columnHeadersFile.getPath());
				String columnHeadersFileName = patternsPath.getName();
				ScalableMapper.LOGGER.info("Reading config file: {}", columnHeadersFileName);
				try {
					this.columnHeaders = DataFeedTools.readColumnHeaders(columnHeadersFileName);
					context.getCounter(ScalableMapper.MapperCounters.COLUMN_COUNT).setValue(this.columnHeaders.size());
				} catch(ParseException e) {
					ScalableMapper.LOGGER.error("There was a problem parsing the column headers!");
				}
			}
		}
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		final String[] columns = value.toString().split(ScalableMapper.FIELD_SEPARATOR, -1);
		if(columns.length != this.columnHeaders.size()) {
			context.getCounter(ScalableMapper.MapperCounters.INPUT_COLUMN_COUNT).setValue(columns.length);
			context.getCounter(ScalableMapper.MapperCounters.CORRUPT_ROW).increment(1);
			return;
		}

		// Create a composite key - post_visid_high:post_visid_low|visit_num|visit_page_num
		final String visIdHigh = DataFeedTools.getValue("post_visid_high", columns, this.columnHeaders);
		final String visIdLow = DataFeedTools.getValue("post_visid_low", columns, this.columnHeaders);
		final String visitNum = DataFeedTools.getValue("visit_num", columns, this.columnHeaders);
		final String visitPageNum = DataFeedTools.getValue("visit_page_num", columns, this.columnHeaders);
		this.key.set(String.format("%s:%s", visIdHigh, visIdLow), Integer.valueOf(visitNum), Integer.valueOf(visitPageNum));
		context.write(this.key, value);
		++ScalableMapper.recordsProcessed;
		if(System.currentTimeMillis() - lastStatusUpdate > 5000) {
			final int duration = (int)((System.currentTimeMillis() - ScalableMapper.lastStatusUpdate) / 1000);
			ScalableMapper.lastStatusUpdate = System.currentTimeMillis();
			context.setStatus("Processed "+ScalableMapper.recordsProcessed+" per "+duration+" seconds.");
			LOGGER.info("Processed "+ScalableMapper.recordsProcessed+" per "+duration+" seconds.");
			ScalableMapper.recordsProcessed = 0;
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
