package com.splunk.shep.testutil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.AssertJUnit;

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
     * @return a temporary, existing, empty file that will be deleted when the
     *         VM terminates.
     * 
     * @see #createTestFilePath()
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
     * @return a temporary file path. The returned file doesn't exist, but the
     *         the path to the file was valid at the time of this method call.
     * 
     * @see #createTestFile()
     */
    public static File createTestFilePath() {
	File testFile = createTestFile();
	AssertJUnit.assertTrue(testFile.delete());
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
	createTempDirectory(dir);
	return dir;
    }

    private static File createNonExistingDirectory() {
	return createNonExistingDirectoryWithPrefix("test-dir");
    }

    private static File createNonExistingDirectoryWithPrefix(String prefix) {
	Class<?> callerToThisMethod = MethodCallerHelper.getCallerToMyMethod();
	String tempDirName = prefix + "-" + callerToThisMethod.getSimpleName();
	File dir = new File(tempDirName);
	while (dir.exists()) {
	    tempDirName += "-" + RandomStringUtils.randomAlphanumeric(2);
	    dir = new File(tempDirName);
	}
	return dir;
    }

    private static void createTempDirectory(File dir) {
	if (!dir.mkdir()) {
	    UtilsTestNG.failForException(
		    "Could not create directory: " + dir.getAbsolutePath(),
		    new RuntimeException());
	}
	dir.deleteOnExit();
    }

    /**
     * Creates a temp directory with a name of the parameter. <br/>
     * Wraps the FileExistException in a {@link RuntimeException} to avoid the
     * try/catch.
     * 
     * @param string
     *            name of the temp directory
     * @return temporary directory that's deleted when the VM terminates.
     */
    public static File createPrefixedTempDirectory(String string) {
	File dir = createNonExistingDirectoryWithPrefix(string);
	if (dir.exists()) {
	    throw new RuntimeException(new FileExistsException());
	}
	createTempDirectory(dir);
	return dir;
    }

    /**
     * Creates directory in the parent with the name of the String. <br/>
     * Note: This file is not temporary and must be removed.
     * 
     * @param parent
     *            where the child directory will live.
     * @param string
     *            name of the child
     * @return the created directory.
     */
    public static File createDirectoryInParent(File parent, String string) {
	File child = new File(parent, string);
	child.mkdir();
	return child;
    }
}
