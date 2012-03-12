package com.splunk.shep.testutil;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.io.FileUtils;
import org.testng.AssertJUnit;

import com.splunk.shep.archiver.model.Bucket;

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
     * Same as calling {@link #assertFileContentsEqual(String, File, File)} with
     * "" as message
     */
    public static void assertFileContentsEqual(File expected, File actual) {
	assertFileContentsEqual("", expected, actual);
    }

    /**
     * Asserts that the contents of specified files are equal. Failing the test
     * with specified message if not.
     */
    public static void assertFileContentsEqual(String message, File expected,
	    File actual) {
	assertTrue(expected.toString() + " doesn't exist.", expected.exists());
	assertTrue(actual.toString() + " doesn't exist.", actual.exists());
	message = message == null ? "" : message;

	try {
	    assertTrue(message, FileUtils.contentEquals(expected, actual));
	} catch (IOException e) {
	    failForException("Can't compare contents of files.", e);
	}
    }

    /**
     * Verifies that two buckets has the same index, format and name.
     * {@link Bucket#equals(Object)} also goes for {@link Bucket#getDirectory()}
     * .getAbsolutePath(), at the time of this implementation.
     */
    public static void assertBucketsGotSameIndexFormatAndName(Bucket bucket,
	    Bucket capturedBucket) {
	assertEquals(bucket.getIndex(), capturedBucket.getIndex());
	assertEquals(bucket.getName(), capturedBucket.getName());
	assertEquals(bucket.getFormat(), capturedBucket.getFormat());
    }
}
