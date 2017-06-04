package com.datafeedtoolbox.examples;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class DataFeedGenerator {
	// Defines how many hits can be in a visit.
	private static final int MAX_VISIT_LENGTH = 150000;

	// Defines how many visits a visitor can have.
	private static final int MAX_VISITS = 3;

	// Defines conversion rate. Number between 0-100. Can be as low as 0.1 (Conversion rate of 0.1%)
	private static final double CONVERSION_RATE = 1;

	// Don't change this -- its used to calculate whether a visitor converted or not
	private static final double DENOMINATOR = (DataFeedGenerator.CONVERSION_RATE * 100);

	// Don't change this. Defines how many digits should be in a visId.
	private static final int VISID_LENGTH = 20;

	public static void main(String[] args) {
		int sum = 0;
		int timesToIterate = 1;
		final String outputFilename = "10MM_datafeed_data.txt";

		// Open the output file
		OutputStream outputStream = null;
		try {
			outputStream = new FileOutputStream(outputFilename);
		} catch(FileNotFoundException e) {
			System.err.println("Unable to open file for writing: "+outputFilename);
			e.printStackTrace();
			System.exit(1);
		}

		try {
			for (int i = 0; i < timesToIterate; ++i) {
				sum += buildTestHits(outputStream, 50000000, "\t", "\n");
			}
		} catch(IOException e) {
			System.err.println("There was a problem writing data to the output stream.");
		}
		System.out.println("Average time: "+(sum/timesToIterate)+"ms");
	}

	/**
	 * Generates a bunch of test hits
	 * @param outputStream The output stream to write the hit data to
	 * @param rowCount The number of rows to generate
	 * @param fieldSeparator The field separator to use. For example, \t
	 * @param lineSeparator The line separator to use. For example, \n
	 * @return The time it took to generate the data
	 */
	public static int buildTestHits(OutputStream outputStream, long rowCount, String fieldSeparator, String lineSeparator) throws IOException {
		// post_visid_high	post_visid_low	visit_num	visit_page_num	post_event_list post_product_list
		StringBuilder hit = new StringBuilder();
		String visIdHigh = "", visIdLow = "";
		int visitNum = 0, visitPageNum = 0, visitNumMax = 0, visitPageNumMax = 0;
		String postEventList = "", postProductList = ";;;;";
		Random rand = new Random();
		final long startTime = System.currentTimeMillis();
		for(long i = 0; i < rowCount; ++i) {
			if(i % 500000 == 0) {
				System.out.println("Generated "+i+" hits out of "+rowCount+". "+((double)i/rowCount)*100+"% complete");
			}
			if(visitPageNum >= visitPageNumMax) {
				visitPageNumMax = 0;
			}
			if(visitPageNumMax == 0) {
				visitPageNumMax = rand.nextInt(DataFeedGenerator.MAX_VISIT_LENGTH)+1;
				visitPageNum = 1;
				++visitNum;
			}
			if(visitNum >= visitNumMax) {
				visitNumMax = 0;
			}
			if(visitNumMax == 0) {
				visitNumMax = rand.nextInt(DataFeedGenerator.MAX_VISITS)+1;
				visitNum = 1;
				visIdHigh = DataFeedGenerator.visIdGenerator();
				visIdLow = DataFeedGenerator.visIdGenerator();
			}
			// Was there an order?
			if(rand.nextInt(10000) < DataFeedGenerator.DENOMINATOR) {
				// End the visit
				visitPageNumMax = 0;
				// Populate the event list and product list columns
				postEventList = "1";
				final int quantity = rand.nextInt(2)+1;
				postProductList = ";Bering Solar Watch;"+quantity+";"+quantity*130.98+";;";
			}
			hit.append(visIdHigh).append(fieldSeparator)
							.append(visIdLow).append(fieldSeparator)
							.append(visitNum).append(fieldSeparator)
							.append(visitPageNum).append(fieldSeparator)
							.append(postEventList).append(fieldSeparator)
							.append(postProductList).append(lineSeparator);
			outputStream.write(hit.toString().getBytes());
			++visitPageNum;
			hit.setLength(0);
			postEventList = "";
			postProductList = ";;;;";
		}
		final long endTime = System.currentTimeMillis();
		return (int)(endTime - startTime);
	}

	/**
	 * Generates a visid. This can be used to generate the value normally found in visid_high
	 * or visid_low columns, which is defined in the documentation as an unsigned 64-bit int.
	 * @return A visid
	 */
	public static String visIdGenerator() {
		// 20 digits
		// Largest possible number 18,446,744,073,709,551,616
		long rawVisid;
		Random generator = new Random();
		rawVisid = generator.nextLong();
		return DataFeedGenerator.addLeadingZeros(
						Long.toUnsignedString(rawVisid),
						DataFeedGenerator.VISID_LENGTH
		);
	}

	/**
	 * Adds leading zeros to a long that's stored in a string. We have to do things this way
	 * because its currently not possible to store a 64-bit unsigned integer in Java. If inLong
	 * has enough digits already without zeros, it is simply returned.
	 * @param inLong The Long that needs padding
	 * @param digits The total number of digits that this number should have. The number of zeros
	 *               is determined by the difference between this number and the length of inLong.
	 * @return The number with leading zeros.
	 */
	private static String addLeadingZeros(String inLong, int digits) {
		if(inLong.length() < digits) {
			int difference = digits - inLong.length();
			StringBuilder padding = new StringBuilder();
			for(int i = 0; i < difference; ++i) {
				padding.append("0");
			}
			return padding.append(inLong).toString();
		}
		return inLong;
	}
}
