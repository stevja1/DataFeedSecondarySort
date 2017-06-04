package com.datafeedtoolbox.examples.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class DataFeedGrouper extends WritableComparator {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataFeedGrouper.class);
	public DataFeedGrouper() {
		super(CompositeDataFeedKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeDataFeedKey key1 = (CompositeDataFeedKey)a;
		CompositeDataFeedKey key2 = (CompositeDataFeedKey)b;
		return key1.getVisId().compareTo(key2.getVisId());
	}
}
