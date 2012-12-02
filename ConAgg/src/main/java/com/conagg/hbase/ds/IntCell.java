package com.conagg.hbase.ds;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Data Structure to convert a int value to bytes
 * 
 * @author komlimobile
 * 
 */
public class IntCell {
	int start;
	byte[] key;

	public IntCell(int start, byte[] key) {
		this.start = start;
		this.key = key;
	}

	public byte[] write(int val) {
		Bytes.putInt(key, start, val);
		return key;
	}

	public int read(byte[] key) {
		return Bytes.toInt(key, start);

	}

}