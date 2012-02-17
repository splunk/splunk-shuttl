package com.splunk.shep.testutil;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class MethodCallerHelperTest {


    @Test(groups = { "fast" })
    public void getCallerToMyMethod_should_return_stackTraceElement_with_thisClass_when_invokingCaller_call() {
	StackTraceElement element = new Caller().call();
	assertEquals(element.getClassName(), getClass().getName());
    }

    private static class Caller {

	public StackTraceElement call() {
	    return new MethodCallerHelper().getCallerToMyMethod();
	}
    }
}
