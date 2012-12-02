package com.conagg.hbase.ds;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Data Structure to convert a float value to bytes
 * 
 * @author komlimobile
 * 
 */
public class FloatCell {

	int start;
	byte[] key;

	public FloatCell(int start, byte[] key) {
		this.start = start;
		this.key = key;
	}

	public byte[] write(float val) {
		Bytes.putDouble(key, start, val);
		return key;
	}

	public long read(byte[] key) {
		return Bytes.toLong(key, start);

	}
}
