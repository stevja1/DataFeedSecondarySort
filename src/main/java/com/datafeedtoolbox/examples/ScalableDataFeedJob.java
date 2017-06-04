package com.datafeedtoolbox.examples;

import com.datafeedtoolbox.examples.secondarysort.CompositeDataFeedKey;
import com.datafeedtoolbox.examples.secondarysort.DataFeedComparator;
import com.datafeedtoolbox.examples.secondarysort.DataFeedGrouper;
import com.datafeedtoolbox.examples.secondarysort.DataFeedPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class ScalableDataFeedJob extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Only "+args.length+" parameters detected. Expected 3.");
			System.err.println("Usage: SumRevenue ColumnHeaders.tsv InHitData.tsv OutputLocation");
			System.exit(2);
		}

		Job job = Job.getInstance(getConf(), ScalableDataFeedJob.class.getCanonicalName());
		job.setJarByClass(ScalableDataFeedJob.class);

		job.setPartitionerClass(DataFeedPartitioner.class);
		job.setGroupingComparatorClass(DataFeedGrouper.class);
		job.setSortComparatorClass(DataFeedComparator.class);

		job.setMapperClass(ScalableMapper.class);
		job.setReducerClass(ScalableReducer.class);

		job.setMapOutputKeyClass(CompositeDataFeedKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.addCacheFile(new Path(args[0]).toUri());
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ScalableDataFeedJob(), args);
		System.exit(exitCode);
	}
}
