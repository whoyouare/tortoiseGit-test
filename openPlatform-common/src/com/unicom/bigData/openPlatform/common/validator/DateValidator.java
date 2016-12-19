package com.unicom.bigData.openPlatform.common.validator;

public class DateValidator implements ColumValidator {

	private static final String PATTERN = "^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}$";

	@Override
	public boolean doValidator(Object object) {
		return ((String) object).matches(PATTERN);
	}
}
