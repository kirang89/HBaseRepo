package com.conagg.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author <a HREF="mailto:kiran.daredevil@gmail.com">Kiran
 *         Gangadharan</a>
 * 
 */
public class ConAggUtils {

	/**
	 * Check whether given date is valid
	 * 
	 * @param dateString
	 * @param format
	 * @return boolean
	 */
	@SuppressWarnings("unused")
	public static boolean isValidDate(String dateString, String format) {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		sdf.setLenient(false);

		try {
			Date date = sdf.parse(dateString);
		} catch (ParseException e) {
			return false;
		}

		return true;
	}

	/**
	 * Check whether given hour is valid
	 * 
	 * @param hourString
	 * @return boolean
	 */
	public static boolean isValidHour(String hourString) {
		try {
			int hour = Integer.parseInt(hourString);
			if (hour > -1 && hour < 60)
				return true;
			else
				return false;
		} catch (Exception e) {
			return false;
		}
	}
}
