package com.splunk.shep.testutil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.io.IOUtils;
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
	FileInputStream expectedFileStream = null;
	FileInputStream actualFileSream = null;
	try {
	    expectedFileStream = new FileInputStream(expected);
	    actualFileSream = new FileInputStream(actual);

	    AssertJUnit.assertTrue(IOUtils.contentEquals(expectedFileStream,
		    actualFileSream));
	} catch (IOException e) {
	    failForException("Can't compare contents of files.", e);
	} finally {
	    IOUtils.closeQuietly(expectedFileStream);
	    IOUtils.closeQuietly(actualFileSream);
	}

    }
}
