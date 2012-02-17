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
