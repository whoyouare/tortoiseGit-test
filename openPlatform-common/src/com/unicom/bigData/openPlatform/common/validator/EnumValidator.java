package com.unicom.bigData.openPlatform.common.validator;

import java.util.HashSet;
import java.util.Set;

public class EnumValidator implements ColumValidator {
	private Set<String> set;

	public EnumValidator() {
	}

	public EnumValidator(String... strings) {
		set = new HashSet<String>();
		for (String string : strings) {
			set.add(string);
		}
	}

	@Override
	public boolean doValidator(Object object) {
		return set.contains(object.toString());
	}
}
