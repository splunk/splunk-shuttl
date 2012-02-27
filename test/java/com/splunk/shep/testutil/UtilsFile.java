package com.splunk.shep.testutil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;

/**
 * All the utils regarding files goes in here. If there are exceptions while
 * doing any operations the test will fail with appropriate message.
 */
public class UtilsFile {

    /**
     * @return a temporary file with random content.
     * 
     * @see UtilsFile#createTestFile()
     * @see UtilsFile#createTestFileWithRandomContent()
     */
    public static File createTestFileWithRandomContent() {
	File testFile = createTestFile();
	populateFileWithRandomContent(testFile);
	return testFile;
    }

    /**
     * @return a temporary file, the file will be deleted when the VM
     *         terminates.
     */
    public static File createTestFile() {
	File testFile = null;
	try {
	    testFile = File.createTempFile(UtilsFile.class.getSimpleName(), "");
	    testFile.deleteOnExit();
	} catch (IOException e) {
	    UtilsTestNG.failForException("Couldn't create a test file.", e);
	}
	return testFile;

    }

    /**
     * Writes random alphanumeric content to specified file.
     * 
     * @param file
     */
    public static void populateFileWithRandomContent(File file) {
	PrintStream printStream = null;
	try {
	    printStream = new PrintStream(file);
	    printStream.println(RandomStringUtils.randomAlphanumeric(1000));
	    printStream.flush();
	} catch (FileNotFoundException e) {
	    UtilsTestNG.failForException("Failed to write random content", e);
	} finally {
	    IOUtils.closeQuietly(printStream);
	}
    }

    /**
     * Creates a temporary directory with a unique name. <br/>
     * The name of the directory includes the file class name of the caller to
     * the method, so it's easier to track leakage if it some how persists
     * between runs.
     * 
     * @return temporary directory that's deleted when the VM terminates.
     */
    public static File createTempDirectory() {
	File dir = createNonExistingDirectory();
	createDirectory(dir);
	dir.deleteOnExit();
	return dir;
    }

    private static File createNonExistingDirectory() {
	Class<?> callerToThisMethod = MethodCallerHelper.getCallerToMyMethod();
	String tempDirName = callerToThisMethod.getSimpleName() + "-test-dir";
	File dir = new File(tempDirName);
	while (dir.exists()) {
	    tempDirName += "-" + RandomStringUtils.randomAlphanumeric(2);
	    dir = new File(tempDirName);
	}
	return dir;
    }

    private static void createDirectory(File dir) {
	if (!dir.mkdir()) {
	    UtilsTestNG.failForException(
		    "Could not create directory: " + dir.getAbsolutePath(),
		    new RuntimeException());
	}
    }
}
