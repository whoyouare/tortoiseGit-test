package com.unicom.bigData.openPlatform.common.hbaseResultMapping;

public class Defaultconverter implements Converter {
    public boolean canProcess(Object object) {
        return true;
    }

    public String convertObjectToString(Object object, Object format) {
        return object == null ? null : object.toString();
    }
}
