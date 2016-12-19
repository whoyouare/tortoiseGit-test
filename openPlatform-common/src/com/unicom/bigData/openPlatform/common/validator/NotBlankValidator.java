package com.unicom.bigData.openPlatform.common.validator;

import org.apache.commons.lang3.StringUtils;

public class NotBlankValidator implements ColumValidator {

	@Override
	public boolean doValidator(Object object) {
		
		return object != null && StringUtils.isNotBlank(object.toString());
	}
}
