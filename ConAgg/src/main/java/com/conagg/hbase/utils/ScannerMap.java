package com.conagg.hbase.utils;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.conagg.constants.Constants;
import com.conagg.hbase.ds.Field;

/**
 * 
 * @author Kiran Gangadharan Organisation: Komli Mobile
 * 
 */
public class ScannerMap {

	/**
	 * 
	 * @param f
	 *            A float value.
	 * @return Incremented and formatted float value
	 */
	private static float incrementFloatValue(float f) {

		String temp = Float.toString(f);
		int integerPlaces = temp.indexOf('.');
		int decimalPlaces = temp.length() - integerPlaces - 1;
		float tempVal = (float) (1 / Math.pow(10, decimalPlaces));
		float newVal = f + tempVal;

		return newVal;
	}

	/**
	 * 
	 * @param d
	 *            A Double value.
	 * @return Incremented and formatted double value.
	 */
	public static double incrementDoubleValue(double d) {

		String temp = Double.toString(d);
		int integerPlaces = temp.indexOf('.');
		int decimalPlaces = temp.length() - integerPlaces - 1;
		float tempVal = (float) (1 / Math.pow(10, decimalPlaces));
		double newVal = (double) (d + tempVal);

		String format = " ";
		for (int i = 0; i < integerPlaces; i++) {
			format += "#";
		}
		format += ".";
		for (int i = 0; i < decimalPlaces; i++) {
			format += "#";
		}

		DecimalFormat df = new DecimalFormat(format);
		Double value = Double.parseDouble(df.format(newVal));

		return value;
	}

	/**
	 * A module that determines whether the data at hand should be
	 * used for partial scanning or applied as filters for getting
	 * data from HBase
	 * 
	 * @param start
	 *            Map that contains the start row key data
	 * 
	 * @param end
	 *            Map that contains the end row key data
	 * 
	 * @param fields
	 *            The row key fields
	 * 
	 * @param types
	 *            The row key types
	 * 
	 * @param startPositions
	 *            The row key start positions
	 * 
	 * @return List<Field> List of fields and their various properties
	 *         name,values,type,action,operator and row key position
	 */
	public static List<Field> getScannerMap(Map<String, Object> start,
			Map<String, Object> end, String[] fields, String[] types,
			String[] startPositions) {

		int pos = 999;
		List<Field> result = new ArrayList<Field>();

		for (int i = 0; i < fields.length; i++) {

			if (start.get(fields[i]) != null && end.get(fields[i]) != null) {

				String sfield = start.get(fields[i]).toString();
				String efield = end.get(fields[i]).toString();

				System.out.println("SFIELD : " + sfield + " : " + "EFIELD : "
						+ efield);

				if (!sfield.equalsIgnoreCase(efield)) {
					System.out.println(fields[i]
							+ " not equal...need to insert param into PS List");

					Field f = new Field();
					f.fieldAction = Constants.PARTIAL_SCAN;
					f.fieldName = fields[i];
					f.fieldOperator1 = null;
					f.fieldOperator2 = null;
					f.fieldType = types[i];
					f.fieldRowKeyPosition = Integer.parseInt(startPositions[i]);

					if (types[i].equalsIgnoreCase(Constants.INTEGER)) {
						f.fieldStartValue = Integer.parseInt(sfield);
						f.fieldEndValue = Integer.parseInt(efield);
					} else if (types[i].equalsIgnoreCase(Constants.LONG)) {
						f.fieldStartValue = Long.parseLong(sfield);
						f.fieldEndValue = Long.parseLong(efield);
					} else if (types[i].equalsIgnoreCase(Constants.DOUBLE)) {
						f.fieldStartValue = Double.parseDouble(sfield);
						f.fieldEndValue = Double.parseDouble(efield);
					} else if (types[i].equalsIgnoreCase(Constants.FLOAT)) {
						f.fieldStartValue = Float.parseFloat(sfield);
						f.fieldEndValue = Float.parseFloat(efield);
					} else if (types[i].equalsIgnoreCase(Constants.STRING)) {
						f.fieldStartValue = sfield.toString();
						f.fieldEndValue = efield.toString();
					}

					// System.out.print(f.fieldName + " " +
					// f.fieldStartValue + " " +
					// f.fieldEndValue);
					result.add(f);

					pos = i;
					for (int j = i + 1; j < fields.length; j++) {
						if (start.get(fields[j]) != null) {
							String sfield1 = start.get(fields[j]).toString();
							String efield1 = end.get(fields[j]).toString();
							if (sfield1 != null && efield1 != null) {

								Field f1 = new Field();

								f1.fieldName = fields[j];
								f1.fieldType = types[j];
								f1.fieldRowKeyPosition =
										Integer.parseInt(startPositions[j]);
								if (types[j]
										.equalsIgnoreCase(Constants.INTEGER)) {
									f1.fieldStartValue =
											Integer.parseInt(sfield1);
									f1.fieldEndValue =
											Integer.parseInt(efield1);
								} else if (types[j]
										.equalsIgnoreCase(Constants.LONG)) {
									f1.fieldStartValue =
											Long.parseLong(sfield1);
									f1.fieldEndValue = Long.parseLong(efield1);
								} else if (types[j]
										.equalsIgnoreCase(Constants.FLOAT)) {
									f1.fieldStartValue =
											Float.parseFloat(sfield1);
									f1.fieldEndValue =
											Float.parseFloat(efield1);
								} else if (types[j]
										.equalsIgnoreCase(Constants.DOUBLE)) {
									f1.fieldStartValue =
											Double.parseDouble(sfield1);
									f1.fieldEndValue =
											Double.parseDouble(efield1);
								} else if (types[j]
										.equalsIgnoreCase(Constants.STRING)) {
									f1.fieldStartValue = sfield1.toString();
									f1.fieldEndValue = efield1.toString();
								}

								if (sfield1.equalsIgnoreCase(efield1)) {
									System.out
											.println("Single value for : "
													+ fields[j]
													+ " need to add to single column value filter");
									f1.fieldAction =
											Constants.SINGLE_COLUMN_SINGLE_VALUE_FILTER;
									f1.fieldOperator1 = Constants.EQ;
									f1.fieldOperator2 = null;

								} else if (sfield1 != efield1) {
									System.out.println(fields[j] + ">="
											+ sfield1);
									System.out.println(fields[j] + "<="
											+ efield1);
									f1.fieldAction =
											Constants.SINGLE_COLUMN_MULTIPLE_VALUE_FILTER;
									f1.fieldOperator1 = Constants.GEQ;
									f1.fieldOperator2 = Constants.LT;
								}

								result.add(f1);
							}
						} else {
							break;
						}
					}
					break;
				} else {
					System.out.println("Write " + fields[i]
							+ " to list with same start and end");
					Field f2 = new Field();
					f2.fieldName = fields[i];
					f2.fieldAction = Constants.PARTIAL_SCAN;
					f2.fieldRowKeyPosition =
							Integer.parseInt(startPositions[i]);
					f2.fieldType = types[i];
					if (types[i].equalsIgnoreCase(Constants.INTEGER)) {
						f2.fieldStartValue = Integer.parseInt(sfield);
						f2.fieldEndValue = Integer.parseInt(efield);
					} else if (types[i].equalsIgnoreCase(Constants.LONG)) {
						f2.fieldStartValue = Long.parseLong(sfield);
						f2.fieldEndValue = Long.parseLong(efield);
					} else if (types[i].equalsIgnoreCase(Constants.FLOAT)) {
						f2.fieldStartValue = Float.parseFloat(sfield);
						f2.fieldEndValue = Float.parseFloat(efield);
					} else if (types[i].equalsIgnoreCase(Constants.DOUBLE)) {
						f2.fieldStartValue = Double.parseDouble(sfield);
						f2.fieldEndValue = Double.parseDouble(efield);
					} else if (types[i].equalsIgnoreCase(Constants.STRING)) {
						f2.fieldStartValue = sfield.toString();
						f2.fieldEndValue = efield.toString();
					}

					result.add(f2);
				}
			} else {
				break;
			}
		}

		if (pos == 999) {
			System.out.println("All values are the same");
			Field f = result.get(result.size() - 1);

			String fieldType = f.fieldType;
			if (fieldType.equalsIgnoreCase(Constants.INTEGER)) {
				f.fieldEndValue = (Integer) f.fieldEndValue + 1;
			} else if (fieldType.equalsIgnoreCase(Constants.LONG)) {
				f.fieldEndValue = (Long) f.fieldEndValue + 1;
			} else if (fieldType.equalsIgnoreCase(Constants.DOUBLE)) {
				f.fieldEndValue =
						incrementDoubleValue((Double) f.fieldEndValue);
			} else if (fieldType.equalsIgnoreCase(Constants.FLOAT)) {
				f.fieldEndValue = incrementFloatValue((Float) f.fieldEndValue);
			} else if (fieldType.equalsIgnoreCase(Constants.STRING)) {
				char[] charValue = f.fieldEndValue.toString().toCharArray();
				charValue[charValue.length - 1] =
						(char) (charValue[charValue.length - 1] + 1);
				String temp = new String(charValue);
				f.fieldEndValue = temp;
			}
		}

		// for (int k = 0; k < result.size(); k++) {
		// Field f = result.get(k);
		// System.out.print(f.fieldName + " : ");
		// System.out.print(f.fieldStartValue + " : ");
		// System.out.print(f.fieldEndValue + " : ");
		// System.out.print(f.fieldType + " : ");
		// System.out.print(f.fieldAction + " : ");
		// System.out.print(f.fieldOperator1 + " : ");
		// System.out.print(f.fieldOperator2 + " ");
		// System.out.print(f.fieldRowKeyPosition + " ");
		// System.out.println();
		// }

		return result;
	}
}
