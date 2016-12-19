package com.unicom.bigData.openPlatform.common.validator;

import java.io.Serializable;

public interface ColumValidator extends Serializable {
    boolean doValidator(Object object);
}
