package com.datafeedtoolbox.examples;

import com.datafeedtoolbox.examples.secondarysort.CompositeDataFeedKey;
import com.datafeedtoolbox.examples.secondarysort.DataFeedPartitioner;
import com.datafeedtoolbox.examples.tools.DataFeedTools;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class DataFeedPartitionerTest {
	@Test
	public void testPartitioner() {
		final DataFeedPartitioner partitioner = new DataFeedPartitioner();
		final List<CompositeDataFeedKey> keys = new ArrayList<>();
		for(int i = 0; i < 10000; ++i) {
			keys.add(new CompositeDataFeedKey(DataFeedGenerator.visIdGenerator(), 1, 1));
		}
		final Text value = new Text("Foo Bar");
		HashMap<Integer, Integer> counts = new HashMap<>();
		int partition;
		for(CompositeDataFeedKey key : keys) {
			partition = partitioner.getPartition(key, value, 4);
			if(counts.containsKey(partition)) {
				counts.put(partition, counts.get(partition) + 1);
			} else {
				counts.put(partition, 1);
			}
		}
		for(Map.Entry<Integer, Integer> entry : counts.entrySet()) {
			System.out.println(String.format("%d : %d", entry.getKey(), entry.getValue()));
		}
	}
}
