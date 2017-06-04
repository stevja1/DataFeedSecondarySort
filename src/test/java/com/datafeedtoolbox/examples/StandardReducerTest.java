package com.datafeedtoolbox.examples;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import static org.junit.Assert.*;
/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class StandardReducerTest {
	@Test
	public void comparatorTest() {
		// post_visid_high	post_visid_low	visit_num	visit_page_num	post_event_list post_product_list
		List<String> columnHeaders = new ArrayList<>();
		columnHeaders.add("post_visid_high");
		columnHeaders.add("post_visid_low");
		columnHeaders.add("visit_num");
		columnHeaders.add("visit_page_num");
		columnHeaders.add("post_event_list");
		columnHeaders.add("post_product_list");
		Comparator<Text> comparator = StandardReducer.getComparator(columnHeaders);
		List<Text> hits = new ArrayList<>();
		hits.add(new Text("ABC\t123\t1\t3\t\t;;;;"));
		hits.add(new Text("ABC\t123\t1\t1\t\t;;;;"));
		hits.add(new Text("ABC\t123\t2\t2\t\t;;;;"));
		hits.add(new Text("ABC\t123\t2\t1\t\t;;;;"));
		hits.add(new Text("ABC\t123\t2\t3\t\t;;;;"));
		hits.add(new Text("ABC\t123\t2\t4\t\t;;;;"));
		hits.add(new Text("ABC\t123\t1\t4\t\t;;;;"));
		hits.add(new Text("ABC\t123\t1\t2\t\t;;;;"));
		hits.sort(comparator);
		assertEquals(hits.get(0), new Text("ABC\t123\t1\t1\t\t;;;;"));
		assertEquals(hits.get(1), new Text("ABC\t123\t1\t2\t\t;;;;"));
		assertEquals(hits.get(2), new Text("ABC\t123\t1\t3\t\t;;;;"));
		assertEquals(hits.get(3), new Text("ABC\t123\t1\t4\t\t;;;;"));
		assertEquals(hits.get(4), new Text("ABC\t123\t2\t1\t\t;;;;"));
		assertEquals(hits.get(5), new Text("ABC\t123\t2\t2\t\t;;;;"));
		assertEquals(hits.get(6), new Text("ABC\t123\t2\t3\t\t;;;;"));
		assertEquals(hits.get(7), new Text("ABC\t123\t2\t4\t\t;;;;"));
	}
}
