package com.unicom.bigData.openPlatform.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 日期工具类
 * 
 * @author gary
 */
public class DateUtil {
	private final static SimpleDateFormat sd1 = new SimpleDateFormat("mm");
	private final static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private final static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy年MM月dd日");
	private final static SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private final static SimpleDateFormat sdf4 = new SimpleDateFormat("yyyy-M-d");
	private final static SimpleDateFormat sdf5 = new SimpleDateFormat("yyyy-MM-dd");
	private final static SimpleDateFormat sdf6 = new SimpleDateFormat("MM-dd");
	private final static SimpleDateFormat sdf7 = new SimpleDateFormat("HH:mm");
	private final static SimpleDateFormat sdf8 = new SimpleDateFormat("yyyyMMddHH");
	private final static SimpleDateFormat sdf9 = new SimpleDateFormat("yyyyMMddHHmm");
	private final static SimpleDateFormat sdf10 = new SimpleDateFormat("yyyy-MM");

	private static final Log logger = LogFactory.getLog(DateUtils.class);

	public static final String YYYYMMDD = "yyyy-MM-dd";

	public static final String YYYYMMDD_ZH = "yyyy年MM月dd日";

	public static final int FIRST_DAY_OF_WEEK = Calendar.MONDAY; // 中国周一是一周的第一天

	/**
	 * 获取系统当前日期：XXXX年X月X日
	 * 
	 * @return
	 */
	public static String currentDate() {
		return sdf2.format(new Date());
	}

	/**
	 * 获取系统当前日期：例子2009-08-08
	 * 
	 * @return
	 */
	public static String currentDate2() {
		return sdf5.format(new Date());
	}

	/**
	 * 获取系统当前日期：例子2009-8-8
	 * 
	 * @return
	 */
	public static String currentDate3() {
		return sdf4.format(new Date());
	}

	/***
	 * 获取系统当前日期：带时分秒
	 * 
	 * @return
	 * @throws ParseException
	 */
	public static Date curDate() throws ParseException {
		Calendar cal = Calendar.getInstance();
		return cal.getTime();
	}

	/**
	 * 获取系统当前日期：不带时分秒
	 * 
	 * @return Date类型
	 * @throws ParseException
	 */
	public static Date curDate5() throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return cal.getTime();
	}

	/**
	 * 获取系统昨日日期：例子2009年08月07日
	 * 
	 * @return
	 */
	public static String yesterdayDate() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 1);
		return convertDate2String2(cal.getTime());
	}

	/**
	 * 获取系统昨日日期：例子2009-08-07
	 * 
	 * @return
	 */
	public static String yesterdayDate2() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.add(Calendar.DATE, -1);
		return convertDate2String5(cal.getTime());
	}

	/**
	 * 获取系统昨日日期：例子2009-8-7
	 * 
	 * @return
	 */
	public static String yesterdayDate3() {

		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 1);
		return convertDate2String4(cal.getTime());
	}

	/**
	 * 获取当前日期的一周前日期，格式为2011-04-04
	 * 
	 * @return
	 */
	public static String weekAgo() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DAY_OF_MONTH, -8);
		return convertDate2String5(cal.getTime());
	}

	/**
	 * 获取当前日期一个月前日期，格式为2011-04-01
	 */
	public static String monthAgo() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DAY_OF_MONTH, -30);
		return convertDate2String5(cal.getTime());
	}

	/**
	 * 将日期转换为凌晨时间：2012-08-08将被转换为2012-08-09 00:00:00,0
	 * 
	 * @param date
	 * @return
	 */
	public static Date convertDateForDawn(Date date) {
		if (date != null) {
			date = DateUtil.addDay(date, 1);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			calendar.add(Calendar.HOUR_OF_DAY, 0);
			calendar.add(Calendar.MINUTE, 0);
			calendar.add(Calendar.SECOND, 0);
			calendar.add(Calendar.MILLISECOND, 0);
			return calendar.getTime();
		} else {
			return null;
		}
	}

	/**
	 * 获取系统当前日期和时间：例子2009-08-08 10:10:01
	 * 
	 * @return
	 */
	public static String currentDateTime() {

		return convertDate2String(new Date());
	}

	/**
	 * 将日期转换成 2009-08-08 10:10:01格式的字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String convertDate2String(Date date) {
		if (date == null) {
			return "";
		}

		return sdf1.format(date);
	}

	/**
	 * 将日期转换成 2009年08月08日 格式的字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String convertDate2String2(Date date) {
		if (date == null) {
			return "";
		}

		return sdf2.format(date);
	}

	/**
	 * 将日期转换成 2009-08-08 10:10:01.988格式的字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String convertDate2String3(Date date) {
		if (date == null) {
			return "";
		}

		return sdf3.format(date);
	}

	/**
	 * 将日期转换成 2009-8-8 格式的字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String convertDate2String4(Date date) {
		if (date == null) {
			return "";
		}

		return sdf4.format(date);
	}

	/**
	 * 将日期转换成 2009-08-08 格式的字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String convertDate2String5(Date date) {
		if (date == null) {
			return "";
		}

		return sdf5.format(date);
	}

	/**
	 * 将日期转换成 08-08 格式的字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String convertDate2String6(Date date) {
		if (date == null) {
			return "";
		}

		return sdf6.format(date);
	}

	/**
	 * 将日期转换成2012- 08 格式的字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String convertDate2String10(Date date) {
		if (date == null) {
			return "";
		}

		return sdf10.format(date);
	}

	/**
	 * 将日期转换成 HH:mm 格式的字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String convertDate2String7(Date date) {
		if (date == null) {
			return "";
		}

		return sdf7.format(date);
	}

	/**
	 * yyyyMMddHH
	 * 
	 * @param time
	 * @return
	 */
	public static String convertDate2String8(Date time) {
		// TODO Auto-generated method stub
		return sdf8.format(time);
	}

	/**
	 * yyyyMMddHHmm
	 * 
	 * @param time
	 * @return
	 */
	public static String convertDate2String9(Date time) {
		// TODO Auto-generated method stub
		return sdf9.format(time);
	}

	/**
	 * mm
	 * 
	 * @param time
	 * @return
	 */
	public static String getMininte(Date time) {
		// TODO Auto-generated method stub
		return sd1.format(time);
	}

	/**
	 * 将字符串“2009-8-8 23:59:50”转换为日期格式
	 * 
	 * @param date
	 * @return
	 */
	public static Date stringToDate(String date) throws Exception {
		if (date == null || "".equals(date))
			return null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-M-d HH:mm:ss");
		return sdf.parse(date);
	}

	/**
	 * 将字符串“2009-8-8”转换为日期格式
	 * 
	 * @param date
	 * @return
	 */
	public static Date stringToDate2(String date) {
		if (date == null || "".equals(date))
			return null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-M-d");
		try {
			return sdf.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * 
	 * @param date
	 *            形如2011-04-01格式的字符串
	 * @return 字符串所表示的日期对象
	 */
	public static Date stringToDate3(String date) {
		if (date == null || "".equals(date))
			return null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			return sdf.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * 
	 * @param date
	 *            形如201104012300格式的字符串
	 * @return 字符串所表示的日期对象
	 */
	public static Date stringToDate4(String date) {
		if (date == null || "".equals(date))
			return null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		try {
			return sdf.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * 将字符串“2009-08-08 23:59:50”转换为日期格式
	 * 
	 * @param date
	 * @return
	 */
	public static Date stringToDate5(String date) throws Exception {
		if (date == null || "".equals(date))
			return null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.parse(date);

	}

	/**
	 * 将字符串“2009-08”转换为日期格式
	 * 
	 * @param date
	 * @return
	 */
	public static Date stringToDate10(String date) throws Exception {
		if (date == null || "".equals(date))
			return null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
		return sdf.parse(date);

	}

	/**
	 * 
	 * @param currentTime想要转换的基准时间
	 * @param days想要转换的天数
	 * @return 返回给定日期、给定天数的前多少天,格式2011-05-11
	 */
	public static String stringSomeDaysAgo(Date currentTime, int days) {
		if (currentTime == null)
			return "";
		Calendar cal = Calendar.getInstance();
		cal.setTime(currentTime);
		cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH) - days);
		return convertDate2String5(cal.getTime());

	}

	/**
	 * 
	 * @param currentTime想要转换的基准时间
	 * @param days想要转换的天数
	 * @return 返回给定日期、给定天数的前多少天,Date对象
	 */
	public static Date dateSomeDaysAgo(Date currentTime, int days) {
		if (currentTime == null)
			return null;
		Calendar cal = Calendar.getInstance();
		cal.setTime(currentTime);
		cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH) - days);
		return cal.getTime();
	}

	/**
	 * 取多少年后的日期
	 * 
	 * @param currentTime
	 * @param years
	 * @return
	 */
	public static String dateSomeYearLaterStr(Date currentTime, int years) {
		if (currentTime == null)
			return "";
		Calendar cal = Calendar.getInstance();
		cal.setTime(currentTime);
		cal.set(Calendar.YEAR, cal.get(Calendar.YEAR) + years);
		return convertDate2String5(cal.getTime());
	}

	public static Date dateSomeYearLaterDate(Date currentTime, int years) {
		if (currentTime == null)
			return null;
		Calendar cal = Calendar.getInstance();
		cal.setTime(currentTime);
		cal.set(Calendar.YEAR, cal.get(Calendar.YEAR) + years);
		return cal.getTime();
	}

	/**
	 * 
	 * @param currentTime想要转换的基准时间
	 * @param days想要转换的天数
	 * @return 返回给定日期、给定天数的前多少天,Date对象
	 */
	public static Date dateSomeMinLater(Date currentTime, int min) {
		if (currentTime == null)
			return null;
		Calendar cal = Calendar.getInstance();
		cal.setTime(currentTime);
		cal.add(Calendar.MINUTE, min);
		return cal.getTime();
	}

	public static String SetDateFormat(String myDate, String strFormat) {

		SimpleDateFormat sdf = new SimpleDateFormat(strFormat);
		String sDate = "";
		try {
			sDate = sdf.format(sdf.parse(myDate));
		} catch (Exception e) {
			e.printStackTrace();
		}

		return sDate;
	}

	/*****************************************
	 * @功能 计算指定日期某年的第几周
	 * @return interger
	 * @throws ParseException
	 ****************************************/
	public static int getWeekNumOfYearDay(String strDate) throws ParseException {
		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date curDate = format.parse(strDate);
		int iWeekNum;
		calendar.setTime(curDate);
		if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
			iWeekNum = calendar.get(Calendar.WEEK_OF_YEAR) - 1;
		} else {
			iWeekNum = calendar.get(Calendar.WEEK_OF_YEAR);
		}
		return iWeekNum;
	}

	/*****************************************
	 * @功能 计算某年某周的开始日期
	 * @return interger
	 * @throws ParseException
	 ****************************************/
	public static String getYearWeekFirstDay(int yearNum, int weekNum) throws ParseException {

		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, yearNum);
		cal.set(Calendar.WEEK_OF_YEAR, weekNum);
		cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		// 分别取得当前日期的年、月、日
		String tempYear = Integer.toString(yearNum);
		String tempMonth = Integer.toString(cal.get(Calendar.MONTH) + 1);
		String tempDay = Integer.toString(cal.get(Calendar.DATE));
		String tempDate = tempYear + "-" + tempMonth + "-" + tempDay;
		return SetDateFormat(tempDate, "yyyy-MM-dd");

	}

	/*****************************************
	 * @功能 计算某年某周的结束日期
	 * @return interger
	 * @throws ParseException
	 ****************************************/
	public static String getYearWeekEndDay(int yearNum, int weekNum) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, yearNum);
		cal.set(Calendar.WEEK_OF_YEAR, weekNum + 1);
		cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		// 分别取得当前日期的年、月、日
		String tempYear = Integer.toString(yearNum);
		String tempMonth = Integer.toString(cal.get(Calendar.MONTH) + 1);
		String tempDay = Integer.toString(cal.get(Calendar.DATE));
		String tempDate = tempYear + "-" + tempMonth + "-" + tempDay;
		return SetDateFormat(tempDate, "yyyy-MM-dd");
	}

	/*****************************************
	 * @功能 计算某年某月的开始日期
	 * @return interger
	 * @throws ParseException
	 ****************************************/
	public static String getYearMonthFirstDay(int yearNum, int monthNum) {

		// 分别取得当前日期的年、月、日
		String tempYear = Integer.toString(yearNum);
		String tempMonth = Integer.toString(monthNum);
		String tempDay = "1";
		String tempDate = tempYear + "-" + tempMonth + "-" + tempDay;
		return SetDateFormat(tempDate, "yyyy-MM-dd");

	}

	/*****************************************
	 * @功能 计算某年某月的结束日期
	 * @return interger
	 * @throws ParseException
	 ****************************************/
	public static String getYearMonthEndDay(int yearNum, int monthNum) {

		// 分别取得当前日期的年、月、日
		String tempYear = Integer.toString(yearNum);
		String tempMonth = Integer.toString(monthNum);
		String tempDay = "31";
		if (tempMonth.equals("1") || tempMonth.equals("3") || tempMonth.equals("5") || tempMonth.equals("7")
				|| tempMonth.equals("8") || tempMonth.equals("10") || tempMonth.equals("12")) {
			tempDay = "31";
		}
		if (tempMonth.equals("4") || tempMonth.equals("6") || tempMonth.equals("9") || tempMonth.equals("11")) {
			tempDay = "30";
		}
		if (tempMonth.equals("2")) {
			if (isLeapYear(yearNum)) {
				tempDay = "29";
			} else {
				tempDay = "28";
			}
		}
		// System.out.println("tempDay:" + tempDay);
		String tempDate = tempYear + "-" + tempMonth + "-" + tempDay;
		return SetDateFormat(tempDate, "yyyy-MM-dd");
	}

	/*****************************************
	 * @功能 判断某年是否为闰年
	 * @return boolean
	 * @throws ParseException
	 ****************************************/
	public static boolean isLeapYear(int yearNum) {
		boolean isLeep = false;
		/** 判断是否为闰年，赋值给一标识符flag */
		if ((yearNum % 4 == 0) && (yearNum % 100 != 0)) {
			isLeep = true;
		} else if (yearNum % 400 == 0) {
			isLeep = true;
		} else {
			isLeep = false;
		}
		return isLeep;
	}

	/*****************************************
	 * @功能 计算某年某周的开始日期
	 * @throws ParseException
	 ****************************************/
	public static String getYearWeekFirstDay(Date curDate) {

		Calendar cal = Calendar.getInstance();
		cal.setTime(curDate);
		if (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
			cal.set(Calendar.WEEK_OF_YEAR, cal.get(Calendar.WEEK_OF_YEAR) - 1);
		} else {
			cal.set(Calendar.WEEK_OF_YEAR, cal.get(Calendar.WEEK_OF_YEAR));
		}
		cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		String tempYear = Integer.toString(cal.get(Calendar.YEAR));
		String tempMonth = Integer.toString(cal.get(Calendar.MONTH) + 1);
		String tempDay = Integer.toString(cal.get(Calendar.DATE));
		String tempDate = tempYear + "-" + tempMonth + "-" + tempDay;
		return SetDateFormat(tempDate, "yyyy-MM-dd");

	}

	public static Date getYearWeekFirstDayDate(Date curDate) {
		return stringToDate3(getYearWeekFirstDay(curDate));
	}

	/*****************************************
	 * @功能 计算某年某周的结束日期
	 * @throws ParseException
	 ****************************************/
	public static String getYearWeekEndDay(Date curDate) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(curDate);
		if (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
			cal.set(Calendar.WEEK_OF_YEAR, cal.get(Calendar.WEEK_OF_YEAR));
		} else {
			cal.set(Calendar.WEEK_OF_YEAR, cal.get(Calendar.WEEK_OF_YEAR) + 1);
		}
		cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		// cal.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY+1);
		// 分别取得当前日期的年、月、日
		String tempYear = Integer.toString(cal.get(Calendar.YEAR));
		String tempMonth = Integer.toString(cal.get(Calendar.MONTH) + 1);
		String tempDay = Integer.toString(cal.get(Calendar.DATE));
		String tempDate = tempYear + "-" + tempMonth + "-" + tempDay;
		return SetDateFormat(tempDate, "yyyy-MM-dd");
	}

	public static Date getYearWeekEndDayDate(Date curDate) {
		return stringToDate3(getYearWeekEndDay(curDate));
	}

	/*****************************************
	 * @功能 计算某年某月的开始日期
	 * @return interger
	 * @throws ParseException
	 ****************************************/
	public static String getYearMonthFirstDay(Date curDate) {
		if (curDate == null)
			return "";
		Calendar cal = Calendar.getInstance();
		cal.setTime(curDate);
		int yearNum = cal.get(Calendar.YEAR);
		int monthNum = cal.get(Calendar.MONTH) + 1;
		return getYearMonthFirstDay(yearNum, monthNum);
	}

	public static Date getYearMonthFirstDayDate(Date curDate) {
		return stringToDate3(getYearMonthFirstDay(curDate));
	}

	/*****************************************
	 * @功能 计算某年某月的结束日期
	 * @return interger
	 * @throws ParseException
	 ****************************************/
	public static String getYearMonthEndDay(Date curDate) {
		if (curDate == null)
			return "";
		Calendar cal = Calendar.getInstance();
		cal.setTime(curDate);
		int yearNum = cal.get(Calendar.YEAR);
		int monthNum = cal.get(Calendar.MONTH) + 1;
		return getYearMonthEndDay(yearNum, monthNum);
	}

	public static Date getYearMonthEndDayDate(Date curDate) {
		return stringToDate3(getYearMonthEndDay(curDate));
	}

	public static String getYearByDate(Date curDate) {
		if (curDate == null)
			return "";
		Calendar cal = Calendar.getInstance();
		cal.setTime(curDate);
		int yearNum = cal.get(Calendar.YEAR);
		return Integer.toString(yearNum);
	}

	public static String getMontByDate(Date curDate) {
		if (curDate == null)
			return "";
		Calendar cal = Calendar.getInstance();
		cal.setTime(curDate);
		int monthNum = cal.get(Calendar.MONTH) + 1;
		return Integer.toString(monthNum);
	}

	public static int getWeekNumOfYearDay(Date curDate) {
		if (curDate == null)
			return 0;
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(curDate);
		int iWeekNum;
		if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
			iWeekNum = calendar.get(Calendar.WEEK_OF_YEAR) - 1;
		} else {
			iWeekNum = calendar.get(Calendar.WEEK_OF_YEAR);
		}
		// int iWeekNum = calendar.get(Calendar.WEEK_OF_YEAR);
		return iWeekNum;
	}

	/**
	 * 在指定日期上加上多少天
	 * 
	 * @param date
	 * @param day
	 * @return
	 */
	public static Date addDay(Date date, int day) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DATE, day);
		return calendar.getTime();
	}

	/**
	 * 在指定日期上加上多少月
	 * 
	 * @param date
	 * @param month
	 * @return
	 */
	public static Date addMonth(Date date, int month) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.MONTH, month);
		return calendar.getTime();
	}

	/**
	 * 在指定日期上加上多少天
	 * 
	 * @param date
	 * @param day
	 * @return
	 */
	public static String addDayString5(Date date, int day) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DATE, day);
		return convertDate2String5(calendar.getTime());
	}

	/**
	 * 在指定日期上加上多少月
	 * 
	 * @param date
	 * @param month
	 * @return
	 */
	public static String addMonthString5(Date date, int month) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.MONTH, month);
		return convertDate2String5(calendar.getTime());
	}

	/**
	 * 比较两个日期差几天
	 * 
	 * @param date1
	 * @param date2
	 * @return
	 */
	public static int compTowData(Date date1, Date date2) {
		String date_str1 = convertDate2String5(date1);
		String date_str2 = convertDate2String5(date2);
		Date d1 = stringToDate3(date_str1);
		Date d2 = stringToDate3(date_str2);
		Long l;
		if (d1.getTime() > d2.getTime()) {
			l = d1.getTime() - d2.getTime();
		} else {
			l = d2.getTime() - d1.getTime();
		}
		String daystr = DurationFormatUtils.formatDuration(l, "d");
		return Integer.parseInt(daystr);
	}

	public static Long compTwoData(String date_str1, String date_str2) {
		// String date_str1 = convertDate2String5(date1);
		// String date_str2 = convertDate2String5(date2);
		Date d1 = stringToDate3(date_str1);
		Date d2 = stringToDate3(date_str2);
		Long l;
		if (d1.getTime() > d2.getTime()) {
			l = d1.getTime() - d2.getTime();
		} else {
			l = d2.getTime() - d1.getTime();
		}
		String day = DurationFormatUtils.formatDuration(l, "d");

		return l;
	}

	/**
	 * 
	 * @param strDate
	 * @return
	 */
	public static Date parseDate(String strDate) {
		return parseDate(strDate, null);
	}

	/**
	 * parseDate
	 * 
	 * @param strDate
	 * @param pattern
	 * @return
	 */
	public static Date parseDate(String strDate, String pattern) {
		Date date = null;
		try {
			if (pattern == null) {
				pattern = YYYYMMDD;
			}
			SimpleDateFormat format = new SimpleDateFormat(pattern);
			date = format.parse(strDate);
		} catch (Exception e) {
			logger.error("parseDate error:" + e);
		}
		return date;
	}

	/**
	 * format date
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate(Date date) {
		return formatDate(date, null);
	}

	/**
	 * format date
	 * 
	 * @param date
	 * @param pattern
	 *            默认格式yyyy-MM-dd
	 * @return
	 */
	public static String formatDate(Date date, String pattern) {
		String strDate = null;
		try {
			if (pattern == null) {
				pattern = YYYYMMDD;
			}
			SimpleDateFormat format = new SimpleDateFormat(pattern);
			strDate = format.format(date);
		} catch (Exception e) {
			logger.error("formatDate error:", e);
		}
		return strDate;
	}

	/**
	 * 取得一年的第几周
	 * 
	 * @param date
	 * @return
	 */
	public static int getWeekOfYear(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		int week_of_year = c.get(Calendar.WEEK_OF_YEAR);
		return week_of_year - 1;
	}

	/**
	 * 计算两个日期间隔的月，天数相减，如果超过15天加一个月，如果间隔（负值）小于等于15天则减一天
	 * 
	 * <pre>
	 * 2012-01-01-2012-02-14 间隔1个月
	 * 2012-01-01-2012-02-15 间隔1个月
	 * 2012-01-01-2012-02-16 间隔2个月
	 * </pre>
	 * 
	 * @param beginDate
	 * @param endDate
	 * @return
	 */
	public static Integer getMonthBetweenBeginDateAndEndDate(Date beginDate, Date endDate) {
		Calendar cal1 = Calendar.getInstance();
		cal1.setTime(beginDate);
		Calendar cal2 = Calendar.getInstance();
		cal2.setTime(endDate);
		int year = cal2.get(Calendar.YEAR) - cal1.get(Calendar.YEAR);
		int month = cal2.get(Calendar.MONTH) - cal1.get(Calendar.MONTH);
		int day = cal2.get(Calendar.DAY_OF_MONTH) - cal1.get(Calendar.DAY_OF_MONTH);
		month = year * 12 + month;
		if (day > 15) {// 超过15天
			month++;
		} else if (day <= -15) {// 间隔小于等于15天
			month--;
		}
		return month;
	}

	/**
	 * 
	 * @desc 方法描述
	 * @param 参数描述
	 * @return 返回值描述
	 * @throws 异常描述
	 * @author:xiaodi@qiyi.com
	 * @date: 2013-3-14
	 */
	public static Integer getMonthsBetweenBeginDateAndEndDate(Date beginDate, Date endDate) {
		Calendar cal1 = Calendar.getInstance();
		cal1.setTime(beginDate);
		Calendar cal2 = Calendar.getInstance();
		cal2.setTime(endDate);
		int year = cal2.get(Calendar.YEAR) - cal1.get(Calendar.YEAR);
		int month = cal2.get(Calendar.MONTH) - cal1.get(Calendar.MONTH);
		month = year * 12 + month;
		month += 1;
		return month;
	}

	/**
	 * getWeekBeginAndEndDate
	 * 
	 * @param date
	 * @param pattern
	 * @return
	 */
	public static String getWeekBeginAndEndDate(Date date, String pattern) {
		Date monday = getMondayOfWeek(date);
		Date sunday = getSundayOfWeek(date);
		return formatDate(monday, pattern) + " - " + formatDate(sunday, pattern);
	}

	/**
	 * 根据日期取得对应周周一日期
	 * 
	 * @param date
	 * @return
	 */
	public static Date getMondayOfWeek(Date date) {
		Calendar monday = Calendar.getInstance();
		monday.setTime(date);
		monday.setFirstDayOfWeek(FIRST_DAY_OF_WEEK);
		monday.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		return monday.getTime();
	}

	/**
	 * 根据日期取得对应周周日日期
	 * 
	 * @param date
	 * @return
	 */
	public static Date getSundayOfWeek(Date date) {
		Calendar sunday = Calendar.getInstance();
		sunday.setTime(date);
		sunday.setFirstDayOfWeek(FIRST_DAY_OF_WEEK);
		sunday.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		return sunday.getTime();
	}

	/**
	 * 取得月的剩余天数
	 * 
	 * @param date
	 * @return
	 */
	public static int getRemainDayOfMonth(Date date) {
		int dayOfMonth = getDayOfMonth(date);
		int day = getPassDayOfMonth(date);
		return dayOfMonth - day;
	}

	/**
	 * 取得月已经过的天数
	 * 
	 * @param date
	 * @return
	 */
	public static int getPassDayOfMonth(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		return c.get(Calendar.DAY_OF_MONTH);
	}

	/**
	 * 取得月天数
	 * 
	 * @param date
	 * @return
	 */
	public static int getDayOfMonth(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		return c.getActualMaximum(Calendar.DAY_OF_MONTH);
	}

	/**
	 * 取得月第一天
	 * 
	 * @param date
	 * @return
	 */
	public static Date getFirstDateOfMonth(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.set(Calendar.DAY_OF_MONTH, c.getActualMinimum(Calendar.DAY_OF_MONTH));
		return c.getTime();
	}

	/**
	 * 取得月最后一天
	 * 
	 * @param date
	 * @return
	 */
	public static Date getLastDateOfMonth(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DAY_OF_MONTH));
		return c.getTime();
	}

	/**
	 * 取得季度第一天
	 * 
	 * @param date
	 * @return
	 */
	public static Date getFirstDateOfSeason(Date date) {
		return getFirstDateOfMonth(getSeasonDate(date)[0]);
	}

	/**
	 * 取得季度第一天
	 * 
	 * @param date
	 * @return
	 */
	public static String getFirstDateOfSeasonStr(Date date) {
		return convertDate2String5(getFirstDateOfMonth(getSeasonDate(date)[0]));
	}

	/**
	 * 取得季度最后一天
	 * 
	 * @param date
	 * @return
	 */
	public static Date getLastDateOfSeason(Date date) {
		return getLastDateOfMonth(getSeasonDate(date)[2]);
	}

	/**
	 * 取得季度最后一天
	 * 
	 * @param date
	 * @return
	 */
	public static String getLastDateOfSeasonStr(Date date) {
		return convertDate2String5(getLastDateOfSeason(getSeasonDate(date)[0]));
	}

	/**
	 * 取得季度天数
	 * 
	 * @param date
	 * @return
	 */
	public static int getDayOfSeason(Date date) {
		int day = 0;
		Date[] seasonDates = getSeasonDate(date);
		for (Date date2 : seasonDates) {
			day += getDayOfMonth(date2);
		}
		return day;
	}

	/**
	 * 取得季度剩余天数
	 * 
	 * @param date
	 * @return
	 */
	public static int getRemainDayOfSeason(Date date) {
		return getDayOfSeason(date) - getPassDayOfSeason(date);
	}

	/**
	 * 取得季度已过天数
	 * 
	 * @param date
	 * @return
	 */
	public static int getPassDayOfSeason(Date date) {
		int day = 0;

		Date[] seasonDates = getSeasonDate(date);

		Calendar c = Calendar.getInstance();
		c.setTime(date);
		int month = c.get(Calendar.MONTH);

		if (month == Calendar.JANUARY || month == Calendar.APRIL || month == Calendar.JULY
				|| month == Calendar.OCTOBER) {// 季度第一个月
			day = getPassDayOfMonth(seasonDates[0]);
		} else if (month == Calendar.FEBRUARY || month == Calendar.MAY || month == Calendar.AUGUST
				|| month == Calendar.NOVEMBER) {// 季度第二个月
			day = getDayOfMonth(seasonDates[0]) + getPassDayOfMonth(seasonDates[1]);
		} else if (month == Calendar.MARCH || month == Calendar.JUNE || month == Calendar.SEPTEMBER
				|| month == Calendar.DECEMBER) {// 季度第三个月
			day = getDayOfMonth(seasonDates[0]) + getDayOfMonth(seasonDates[1]) + getPassDayOfMonth(seasonDates[2]);
		}
		return day;
	}

	/**
	 * 取得季度月
	 * 
	 * @param date
	 * @return
	 */
	public static Date[] getSeasonDate(Date date) {
		Date[] season = new Date[3];

		Calendar c = Calendar.getInstance();
		c.setTime(date);

		int nSeason = getSeason(date);
		if (nSeason == 1) {// 第一季度
			c.set(Calendar.MONTH, Calendar.JANUARY);
			season[0] = c.getTime();
			c.set(Calendar.MONTH, Calendar.FEBRUARY);
			season[1] = c.getTime();
			c.set(Calendar.MONTH, Calendar.MARCH);
			season[2] = c.getTime();
		} else if (nSeason == 2) {// 第二季度
			c.set(Calendar.MONTH, Calendar.APRIL);
			season[0] = c.getTime();
			c.set(Calendar.MONTH, Calendar.MAY);
			season[1] = c.getTime();
			c.set(Calendar.MONTH, Calendar.JUNE);
			season[2] = c.getTime();
		} else if (nSeason == 3) {// 第三季度
			c.set(Calendar.MONTH, Calendar.JULY);
			season[0] = c.getTime();
			c.set(Calendar.MONTH, Calendar.AUGUST);
			season[1] = c.getTime();
			c.set(Calendar.MONTH, Calendar.SEPTEMBER);
			season[2] = c.getTime();
		} else if (nSeason == 4) {// 第四季度
			c.set(Calendar.MONTH, Calendar.OCTOBER);
			season[0] = c.getTime();
			c.set(Calendar.MONTH, Calendar.NOVEMBER);
			season[1] = c.getTime();
			c.set(Calendar.MONTH, Calendar.DECEMBER);
			season[2] = c.getTime();
		}
		return season;
	}

	/**
	 * 
	 * 1 第一季度 2 第二季度 3 第三季度 4 第四季度
	 * 
	 * @param date
	 * @return
	 */
	public static int getSeason(Date date) {

		int season = 0;

		Calendar c = Calendar.getInstance();
		c.setTime(date);
		int month = c.get(Calendar.MONTH);
		switch (month) {
		case Calendar.JANUARY:
		case Calendar.FEBRUARY:
		case Calendar.MARCH:
			season = 1;
			break;
		case Calendar.APRIL:
		case Calendar.MAY:
		case Calendar.JUNE:
			season = 2;
			break;
		case Calendar.JULY:
		case Calendar.AUGUST:
		case Calendar.SEPTEMBER:
			season = 3;
			break;
		case Calendar.OCTOBER:
		case Calendar.NOVEMBER:
		case Calendar.DECEMBER:
			season = 4;
			break;
		default:
			break;
		}
		return season;
	}

	public static int compareDateDay(String DATE1, String DATE2) {

		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date dt1 = df.parse(DATE1);
			Date dt2 = df.parse(DATE2);
			if (dt1.getTime() > dt2.getTime()) {
				return 1;
			} else {
				return 0;
			}
		} catch (Exception exception) {
			exception.printStackTrace();
		}
		return 0;
	}

	/**
	 * 大于返回1，小于返回0
	 * 
	 * @desc 方法描述
	 * @param 参数描述
	 * @return 返回值描述
	 * @throws 异常描述
	 * @author:xiaodi@qiyi.com
	 * @date: 2013-3-15
	 */
	public static int compareDateDay(Date DATE1, Date DATE2) {

		try {
			if (DATE1.getTime() > DATE2.getTime()) {
				return 1;
			} else {
				return 0;
			}
		} catch (Exception exception) {
			exception.printStackTrace();
		}
		return 0;
	}

	/**
	 * 
	 * 获取两日期之间的天数
	 * 
	 * @param date
	 * @return
	 */
	public static int getDateBeginAndEndDate(Date DATE1, Date DATE2) {
		try {
			long time = DATE2.getTime() - DATE1.getTime();
			return (int) (time / 86400000 + 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * 
	 * 获取两日期之间的天数
	 * 
	 * @param date
	 * @return
	 */
	public static int getDateBeginAndEndDate(String DATE1, String DATE2) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date dt1 = df.parse(DATE1);
			Date dt2 = df.parse(DATE2);
			int time = getDateBeginAndEndDate(dt1, dt2);
			return time;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public static void main(String args[]) {
		// System.out.println("getFirstDateOfSeason=" + getFirstDateOfSeason(new
		// Date()));
		// System.out.println("getFirstDateOfMonth=" + getFirstDateOfMonth(new
		// Date()));
		// System.out.println("getFirstDateOfMonth=" + getFirstDateOfMonth(new
		// Date()));
		// System.out.println("getFirstDateOfSeasonStr=" +
		// getFirstDateOfSeasonStr(new Date()));
		// System.out.println("getLastDateOfSeasonStr=" +
		// getLastDateOfSeasonStr(new Date()));
		// System.out.println("getYearMonthFirstDay=" + getYearMonthFirstDay(new
		// Date()));
		// System.out.println("getYearWeekEndDay=" + getYearWeekEndDay(new
		// Date()));
		// System.out.println("getYearWeekFirstDay=" + getYearWeekFirstDay(new
		// Date()));
		System.out.println(getDateBeginAndEndDate(new Date(), getLastDateOfMonth(new Date())));

	}

}
