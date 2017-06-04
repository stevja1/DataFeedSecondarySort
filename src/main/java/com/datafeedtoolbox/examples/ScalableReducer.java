package com.datafeedtoolbox.examples;

import com.datafeedtoolbox.examples.secondarysort.CompositeDataFeedKey;
import com.datafeedtoolbox.examples.tools.DataFeedTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class ScalableReducer extends Reducer<CompositeDataFeedKey,Text,Text,DoubleWritable> {
	private final Text visId = new Text();
	private final DoubleWritable result = new DoubleWritable();
	private Configuration conf;
	private List<String> columnHeaders;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		this.conf = context.getConfiguration();
		URI[] columnHeadersFiles = Job.getInstance(conf).getCacheFiles();
		if(columnHeadersFiles != null && columnHeadersFiles.length > 0) {
			for (URI columnHeadersFile : columnHeadersFiles) {
				Path patternsPath = new Path(columnHeadersFile.getPath());
				String patternsFileName = patternsPath.getName();
				try {
					this.columnHeaders = DataFeedTools.readColumnHeaders(patternsFileName);
				} catch(ParseException e) {
					System.err.println("There was a problem parsing the column headers!");
				}
			}
		}
	}

	@Override
	protected void reduce(CompositeDataFeedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// The hit data stored in 'values' is already sorted. Yay!
		String[] columns;
		String eventList;
		String productList;
		Double revenue = 0.0;

		for(Text hit : values) {
			columns = hit.toString().split("\\t", -1);
			eventList = DataFeedTools.getValue("post_event_list", columns, this.columnHeaders);
			eventList = String.format(",%s,", eventList);
			// Was there a purchase?
			if(eventList.contains(",1,")) {
				productList = DataFeedTools.getValue("post_product_list", columns, this.columnHeaders);
				revenue += DataFeedTools.calculateRevenue(productList);
			}
		}

		// Let's just grab the visId part of the key before we return it. No need to return the
		// composite key that was built by the mapper.
		this.visId.set(key.getVisId());
		this.result.set(revenue);
		context.write(this.visId, this.result);
	}
}
