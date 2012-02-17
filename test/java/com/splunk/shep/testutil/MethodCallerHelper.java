package com.splunk.shep.testutil;

public class MethodCallerHelper {

    public StackTraceElement getCallerToMyMethod() {
	int indexOfCallerToTheirMethod = 4;
	return Thread.getAllStackTraces().get(Thread.currentThread())[indexOfCallerToTheirMethod];
    }

}
