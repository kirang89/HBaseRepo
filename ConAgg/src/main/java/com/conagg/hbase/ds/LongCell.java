package com.conagg.hbase.ds;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Data Structure to convert a long value to bytes
 * 
 * @author komlimobile
 * 
 */
public class LongCell {
	int start;
	byte[] key;

	public LongCell(int start, byte[] key) {
		this.start = start;
		this.key = key;
	}

	public byte[] write(long val) {
		Bytes.putLong(key, start, val);
		return key;
	}

	public long read(byte[] key) {
		return Bytes.toLong(key, start);

	}
}
