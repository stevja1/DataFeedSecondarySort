package com.datafeedtoolbox.examples;

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
import java.util.Comparator;
import java.util.List;

public class StandardReducer extends Reducer<Text,Text,Text,DoubleWritable> {
	private final DoubleWritable result = new DoubleWritable();
	private Configuration conf;
	private List<String> columnHeaders;
	private Comparator<Text> comparator;

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
					this.comparator = StandardReducer.getComparator(this.columnHeaders);
				} catch(ParseException e) {
					System.err.println("There was a problem parsing the column headers!");
				}
			}
		}
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String[] columns;
		String eventList;
		String productList;
		Double revenue = 0.0;
		List<Text> visitorTraffic = new ArrayList<>();

		// Read data into an ArrayList that can be sorted
		for(Text value : values) {
			visitorTraffic.add(new Text(value));
		}

		visitorTraffic.sort(this.comparator);

		for(Text hit : visitorTraffic) {
			columns = hit.toString().split("\\t", -1);
			eventList = DataFeedTools.getValue("post_event_list", columns, this.columnHeaders);
			eventList = String.format(",%s,", eventList);
			// Was there a purchase?
			if(eventList.contains(",1,")) {
				productList = DataFeedTools.getValue("post_product_list", columns, this.columnHeaders);
				revenue += DataFeedTools.calculateRevenue(productList);
			}
		}
		this.result.set(revenue);
		context.write(key, this.result);
	}

	public static Comparator<Text> getComparator(List<String> columnHeaders) {
		return new Comparator<Text>() {
			@Override
			public int compare(Text o1, Text o2) {
				// Parse the visit_num and visit_page_num columns out of both hits
				String[] hit1Columns = o1.toString().split("\\t", -1);
				int hit1VisitNum = Integer.valueOf(DataFeedTools.getValue("visit_num", hit1Columns, columnHeaders));
				int hit1VisitPageNum = Integer.valueOf(DataFeedTools.getValue("visit_page_num", hit1Columns, columnHeaders));
				String[] hit2Columns = o2.toString().split("\\t", -1);
				int hit2VisitNum = Integer.valueOf(DataFeedTools.getValue("visit_num", hit2Columns, columnHeaders));
				int hit2VisitPageNum = Integer.valueOf(DataFeedTools.getValue("visit_page_num", hit2Columns, columnHeaders));

				// Place them inside of a number that can be easily compared
				// In this case, we're using a double. Data will be formatted like this: 1.1, 1.2, 1.3
				// where the number left of the decimal is the visit_num and the number right of the
				// decimal is the visit_page_num.
				double hit1Sequence = Double.valueOf(String.format("%d.%d", hit1VisitNum, hit1VisitPageNum));
				double hit2Sequence = Double.valueOf(String.format("%d.%d", hit2VisitNum, hit2VisitPageNum));
				// Now compare. Return -1 if o1 is before o2. Return 0 if they're equal (should never happen),
				// return 1 if o1 is after o2.
				if(hit1Sequence > hit2Sequence) {
					return 1;
				} else if(hit1Sequence < hit2Sequence) {
					return -1;
				} else return 0;
			}
		};
	}
}