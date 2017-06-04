package com.datafeedtoolbox.examples.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class DataFeedComparator extends WritableComparator {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataFeedComparator.class);

	public DataFeedComparator() {
		super(CompositeDataFeedKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeDataFeedKey key1 = (CompositeDataFeedKey)a;
		CompositeDataFeedKey key2 = (CompositeDataFeedKey)b;

		final int result = key1.getVisId().compareTo(key2.getVisId());
		if(result == 0) {
			if(key1.getHitOrder().get() < key2.getHitOrder().get()) return -1;
			else if(key1.getHitOrder().get() > key2.getHitOrder().get()) return 1;
			else return 0;
		} else return result;
	}
}
