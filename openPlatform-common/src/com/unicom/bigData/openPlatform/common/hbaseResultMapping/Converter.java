package com.unicom.bigData.openPlatform.common.hbaseResultMapping;

public interface Converter {
    boolean canProcess(Object object);
    String convertObjectToString(Object object, Object format);
}
