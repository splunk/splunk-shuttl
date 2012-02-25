package com.splunk.shep.testutil;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.testng.AssertJUnit;

/**
 * All the utils regarding testng goes in here.
 */
public class UtilsTestNG {

    /**
     * Fails a test with specified message and the stacktrace of specified
     * exception.
     */
    public static void failForException(String message, Exception exception) {
	StringBuilder failMessage = new StringBuilder();

	failMessage.append(message);
	failMessage.append("\n");
	failMessage.append("Failed because of ");
	failMessage.append(getStackTrace(exception));

	AssertJUnit.fail(failMessage.toString());
    }

    private static String getStackTrace(Exception exception) {
	StringWriter stackTraceStringWriter = new StringWriter();
	PrintWriter printWriter = new PrintWriter(stackTraceStringWriter);
	exception.printStackTrace(printWriter);
	printWriter.flush();
	return stackTraceStringWriter.toString();
    }

}
