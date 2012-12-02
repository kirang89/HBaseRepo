package com.conagg.hbase.utils;

import java.text.ParseException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.conagg.hbase.ds.DateCell;
import com.conagg.hbase.ds.DoubleCell;
import com.conagg.hbase.ds.FloatCell;
import com.conagg.hbase.ds.IntCell;
import com.conagg.hbase.ds.LongCell;
import com.conagg.hbase.ds.StringCell;

/**
 * @author <a HREF="mailto:kiran.daredevil@gmail.com">Kiran
 *         Gangadharan</a>
 * 
 */
public class RowKeyGenerator {

	final static int INT_CELL = 1;
	final static int LONG_CELL = 2;
	final static int STRING_CELL = 3;
	final static int DATE_CELL = 4;
	final static int DOUBLE_CELL = 5;
	final static int FLOAT_CELL = 6;

	byte[] rowKey;

	/**
	 * 
	 */
	public RowKeyGenerator() {
		rowKey = null;
	}

	/**
	 * @param len
	 */
	public void setRowKeyLength(int len) {
		rowKey = new byte[len];
	}

	/**
	 * @param keyMap
	 * @return
	 * @throws ParseException
	 */
	public byte[] getKey(Map<String, List<Object>> keyMap)
			throws ParseException {
		Iterator<Entry<String, List<Object>>> it = keyMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, List<Object>> pairs = it.next();
			List<Object> value = pairs.getValue();
			Object data = value.get(0);
			int type = (Integer) value.get(2);
			int startPos = (Integer) value.get(1);
			switch (type) {
				case INT_CELL:
					IntCell intCell = new IntCell(startPos, rowKey);
					rowKey = intCell.write((Integer) data);
					break;
				case LONG_CELL:
					LongCell longCell = new LongCell(startPos, rowKey);
					rowKey = longCell.write((Long) data);
					break;
				case STRING_CELL:
					StringCell stringCell =
							new StringCell(startPos, rowKey, data.toString()
									.length());
					rowKey = stringCell.write(data.toString());
					break;
				case DATE_CELL:
					DateCell dateCell = new DateCell(startPos, rowKey);
					rowKey = dateCell.write(data.toString());
					break;
				case DOUBLE_CELL:
					DoubleCell doubleCell = new DoubleCell(startPos, rowKey);
					rowKey = doubleCell.write((Double) data);
					break;
				case FLOAT_CELL:
					FloatCell floatCell = new FloatCell(startPos, rowKey);
					rowKey = floatCell.write((Float) data);
					break;
			}
		}

		// System.out.println("Final Row Key : " +
		// Arrays.toString(rowKey));
		return rowKey;

	}

}
