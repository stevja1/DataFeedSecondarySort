package com.datafeedtoolbox.examples.secondarysort;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Copyright Jared Stevens 2017 All Rights Reserved
 */
public class CompositeDataFeedKey implements WritableComparable<CompositeDataFeedKey> {
	private final Text visId = new Text();
	private final IntWritable visitNum = new IntWritable();
	private final IntWritable visitPageNum = new IntWritable();
	private final DoubleWritable hitOrder = new DoubleWritable();

	public CompositeDataFeedKey() {
		this.visId.set("");
		this.visitNum.set(0);
		this.visitPageNum.set(0);
		this.hitOrder.set(0.0);
	}

	public CompositeDataFeedKey(String visId, int visitNum, int visitPageNum) {
		this.visId.set(visId);
		this.visitNum.set(visitNum);
		this.visitPageNum.set(visitPageNum);
		this.hitOrder.set(Double.valueOf(String.format("%d.%d", visitNum, visitPageNum)));
	}

	public void set(String visId, int visitNum, int visitPageNum) {
		this.visId.set(visId);
		this.visitNum.set(visitNum);
		this.visitPageNum.set(visitPageNum);
		this.hitOrder.set(Double.valueOf(String.format("%d.%d", visitNum, visitPageNum)));
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		this.visId.write(dataOutput);
		this.visitNum.write(dataOutput);
		this.visitPageNum.write(dataOutput);
		this.hitOrder.write(dataOutput);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.visId.readFields(dataInput);
		this.visitNum.readFields(dataInput);
		this.visitPageNum.readFields(dataInput);
		this.hitOrder.readFields(dataInput);
	}

	@Override
	public int compareTo(CompositeDataFeedKey o) {
		final int result = this.getVisId().compareTo(o.getVisId());
		if(result == 0) {
			if(this.getHitOrder().get() < o.getHitOrder().get()) return -1;
			else if(this.getHitOrder().get() > o.getHitOrder().get()) return 1;
			else return 0;
		} else return result;
	}

	public Text getVisId() {
		return visId;
	}

	public void setVisId(Text visId) {
		this.visId.set(visId);
	}

	public IntWritable getVisitNum() {
		return visitNum;
	}

	public void setVisitNum(IntWritable visitNum) {
		this.visitNum.set(visitNum.get());
	}

	public IntWritable getVisitPageNum() {
		return visitPageNum;
	}

	public void setVisitPageNum(IntWritable visitPageNum) {
		this.visitPageNum.set(visitPageNum.get());
	}

	public DoubleWritable getHitOrder() {
		return hitOrder;
	}

	public void setHitOrder(DoubleWritable hitOrder) {
		this.hitOrder.set(hitOrder.get());
	}
}
