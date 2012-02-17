package com.splunk.shep.testutil;

public class MethodCallerHelper {

    private final int INDEX_OF_CALLER_TO_THIS_METHOD = 3;

    public StackTraceElement getCallerToMyMethod() {
	StackTraceElement[] elements = Thread.getAllStackTraces().get(
		Thread.currentThread());
	return getCallerToMyMethod(elements);
    }

    protected StackTraceElement getCallerToMyMethod(StackTraceElement[] elements) {
	StackTraceElement caller = elements[INDEX_OF_CALLER_TO_THIS_METHOD];
	return getClassThatCalledCaller(elements, caller);
    }

    private StackTraceElement getClassThatCalledCaller(
	    StackTraceElement[] elements, StackTraceElement caller) {
	for (int i = INDEX_OF_CALLER_TO_THIS_METHOD; i < elements.length; i++)
	    if (!isSameClassAsCaller(caller, elements[i]))
		return elements[i];
	return null;
    }

    private boolean isSameClassAsCaller(StackTraceElement caller,
	    StackTraceElement element) {
	return element.getClassName().equals(caller.getClassName());
    }

}
