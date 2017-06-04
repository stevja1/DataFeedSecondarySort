package com.datafeedtoolbox.examples.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class DataFeedTools {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataFeedTools.class);

	/**
	 * Reads in the column headers from a configuration file.
	 * @param fileName The filename containing the column headers
	 * @throws IOException Thrown if there is a problem reading from the file
	 * @throws ParseException Thrown if there is a problem with the column header format.
	 */
	public static List<String> readColumnHeaders(final String fileName) throws IOException, ParseException {
		List<String> columnHeaders = new ArrayList<>();
		final BufferedReader fis = new BufferedReader(new FileReader(fileName));
		final String line = fis.readLine();
		if(line != null && line.length() > 0 && line.contains("\t")) {
			final String[] columns = line.split("\\t", -1);
			columnHeaders.addAll(Arrays.asList(columns));
			/*
			for(String columnHeader : columns) {
				columnHeaders.add(columnHeader);
			}
			*/
		} else {
			throw new ParseException("There was a problem reading the column headers!", 0);
		}
		return columnHeaders;
	}

	public static String getValue(String columnName, String[] columns, List<String> columnHeaders) {
		int index = columnHeaders.lastIndexOf(columnName);
		if(index < 0) {
			LOGGER.warn("There was an error locating column "+columnName+ " in column headers.");
			return "";
		} else {
			return columns[index];
		}
	}

	/**
	 * Parses a product list. Sums the revenue and returns it.
	 * @param productList The product list
	 * @return The total revenue represented by the products in the provided product list
	 */
	public static double calculateRevenue(String productList) {
		final String PRODUCT_ITEM_DELIM = ",";
		final String PRODUCT_PART_DELIM = ";";
		double revenue = 0.0;
		final String[] productItems = productList.split(PRODUCT_ITEM_DELIM, -1);
		String[] productParts;
		for(String productItem : productItems) {
			productParts = productItem.split(PRODUCT_PART_DELIM, -1);
			// Fields:
			// 0: Product Category
			// 1: Product SKU
			// 2: Units
			// 3: Total Revenue
			revenue += Double.valueOf(productParts[3]);
		}
		return revenue;
	}
}
