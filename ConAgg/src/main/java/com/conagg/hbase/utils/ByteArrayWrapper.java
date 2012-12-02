package com.conagg.hbase.utils;

import java.util.Arrays;

/**
 * A class to wrap a byte array
 * 
 * @author <a HREF="mailto:kiran.daredevil@gmail.com">Kiran
 *         Gangadharan</a>
 * 
 */
public final class ByteArrayWrapper {
	private final byte[] data;

	/**
	 * Constructor
	 * 
	 * @param data
	 */
	public ByteArrayWrapper(byte[] data) {
		if (data == null) {
			throw new NullPointerException();
		}
		this.data = data;
	}

	/**
	 * Returns the byte array
	 * 
	 * @return byte array
	 */
	public byte[] getByteArray() {
		return data;
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof ByteArrayWrapper)) {
			return false;
		}
		return Arrays.equals(data, ((ByteArrayWrapper) other).data);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(data);
	}
}
