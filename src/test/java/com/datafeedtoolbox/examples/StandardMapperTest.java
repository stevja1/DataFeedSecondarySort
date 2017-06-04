package com.datafeedtoolbox.examples;

import org.junit.Test;

import java.security.NoSuchAlgorithmException;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class StandardMapperTest {
	@Test
	public void testIncludeHit() throws NoSuchAlgorithmException {
		String visIdHigh, visIdLow;
		final double sampleRate = 0.1;
		boolean includeHit;
		double includedHits = 0;
		final double sampleSize = 100000;
		for(int i = 0; i < sampleSize; ++i) {
			visIdHigh = DataFeedGenerator.visIdGenerator();
			visIdLow = DataFeedGenerator.visIdGenerator();
			includeHit = StandardMapper.sampleHit(visIdHigh, visIdLow, sampleRate);
			if(includeHit) {
				++includedHits;
			}
		}
		System.out.println(String.format("Rate: %f out of %f", includedHits, sampleSize));
		final double observedRate = (includedHits / sampleSize) * 100;
		System.out.println("Observed Rate: "+observedRate+"%");
		System.out.println("Specified Rate: "+sampleRate+"%");
	}
}
