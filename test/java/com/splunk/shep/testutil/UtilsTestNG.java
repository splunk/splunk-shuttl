package com.splunk.shep.testutil;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.io.FileUtils;
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

    /**
     * Asserts that the contents of specified files are equal.
     */
    public static void assertFileContentsEqual(File expected, File actual) {
	assertTrue(expected.toString() + " doesn't exist.", expected.exists());
	assertTrue(actual.toString() + " doesn't exist.", actual.exists());
	
	try {
	    assertTrue(FileUtils.contentEquals(expected, actual));
	} catch (IOException e) {
	    failForException("Can't compare contents of files.", e);
	}

    }
}
