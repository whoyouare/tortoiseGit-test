package com.unicom.bigData.openPlatform.common.validator;

import org.apache.commons.lang3.math.NumberUtils;

public class NumValditor implements ColumValidator {

	@Override
	public boolean doValidator(Object object) {
		return object == null || NumberUtils.isNumber(object.toString());
	}
	
}