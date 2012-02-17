package com.splunk.shep.testutil;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class MethodCallerHelperTest {

    @Test(groups = { "fast" })
    public void getCallerToMyMethod_should_return_stackTraceElement_with_thisClass_when_invokingCaller_call() {
	StackTraceElement element = new Caller().call();
	assertEquals(element.getClassName(), getClass().getName());
    }

    @Test(groups = { "fast" })
    public void getCallerToMyMethod_should_stillGetThisClass_when_otherInternalCallsInsideTheClassIsCalled() {
	StackTraceElement element = new Caller().internalCall_before_call();
	assertEquals(element.getClassName(), getClass().getName());
    }

    @Test(groups = { "fast" }, expectedExceptions = { IllegalStateException.class })
    public void should_throw_IllegalStateException_when_stackTrace_containsOnlyTheSameClass() {
	StackTraceElement[] elements = getElementsWithSameClass();
	new MethodCallerHelper().getCallerToMyMethod(elements);
    }

    private StackTraceElement[] getElementsWithSameClass() {
	StackTraceElement[] elements = new StackTraceElement[MethodCallerHelper.INDEX_OF_CALLER_TO_THIS_METHOD + 1];
	for (int i = 0; i < elements.length; i++)
	    elements[i] = new StackTraceElement("class", "methodName",
		    "fileName", 0);
	return elements;
    }

    private static class Caller {

	public StackTraceElement internalCall_before_call() {
	    return internalCall();
	}

	public StackTraceElement internalCall() {
	    return call();
	}

	public StackTraceElement call() {
	    return new MethodCallerHelper().getCallerToMyMethod();
	}
    }
}
