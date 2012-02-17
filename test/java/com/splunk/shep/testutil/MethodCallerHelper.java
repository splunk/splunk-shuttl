package com.splunk.shep.testutil;

/**
 * MethodCallerHelper is a class for finding out who outside of your own class,
 * called you. <br>
 * See the test in {@link MethodCallerHelperTest} to get an understanding for
 * the class.
 * 
 * @author periksson@splunk.com
 * 
 */
public class MethodCallerHelper {

    protected static final int INDEX_OF_CALLER_TO_THIS_METHOD = 3;

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
	throw new IllegalStateException(
		"There should always be a class that called the caller. "
			+ "Otherwise this code would be run by the Thread class directly, "
			+ "which would be wierd.");
    }

    private boolean isSameClassAsCaller(StackTraceElement caller,
	    StackTraceElement element) {
	return element.getClassName().equals(caller.getClassName());
    }

}
