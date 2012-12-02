package com.conagg.hbase.ds;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Data Structure to convert a String value to bytes
 * 
 * @author komlimobile
 * 
 */
public class StringCell {

	int start, width;
	byte[] key;

	public StringCell(int start, byte[] key, int width) {
		this.start = start;
		this.key = key;
		this.width = width;
	}

	public byte[] write(String val) {
		for (int i = 0; i < width && i < val.length(); i++) {
			key[start + i] = (byte) val.charAt(i);

		}
		return key;
	}

	public String read(byte[] key) {
		byte[] arr = new byte[width];
		System.arraycopy(key, start, arr, 0, width);
		return Bytes.toString(arr);
	}
}
