package com.datafeedtoolbox.examples.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class DataFeedPartitioner extends Partitioner<CompositeDataFeedKey, Text> {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataFeedPartitioner.class);
	@Override
	public int getPartition(CompositeDataFeedKey key, Text value, int partitions) {
		return (key.getVisId().hashCode() & Integer.MAX_VALUE) % partitions;
	}
}
