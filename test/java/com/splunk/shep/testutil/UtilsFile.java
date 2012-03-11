package com.splunk.shep.testutil;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.FileUtils;
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
	File dir = getUniquelyNamedFile();
	createDirectory(dir);
	return dir;
    }

    /**
     * Creates a directory by first invoking createTempDirectory() and then
     * creating a new directory inside that with specified name. This is done
     * the prevent collisions when same name is used.
     * 
     * @param name
     *            The name of the directory to be created.
     * @return A directory with specified name that will be removed on JVM exit
     */
    public static File createTmpDirectoryWithName(String name) {
	File tmpDirectory = new File(createTempDirectory(), name);
	createDirectory(tmpDirectory);
	try {
	    FileUtils.forceDeleteOnExit(tmpDirectory);
	} catch (IOException e) {
	    UtilsTestNG.failForException(
		    "Could not force delete tmpDirectory: " + tmpDirectory, e);
	}
	return tmpDirectory;
    }

    private static File getUniquelyNamedFile() {
	return getUniquelyNamedFileWithPrefix("test-dir");
    }

    private static File getUniquelyNamedFileWithPrefix(String prefix) {
	Class<?> callerToThisMethod = MethodCallerHelper.getCallerToMyMethod();
	String tempDirName = FileUtils.getUserDirectoryPath() + File.separator
		+ prefix + "-" + callerToThisMethod.getSimpleName();
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
	try {
	    FileUtils.forceDeleteOnExit(dir);
	} catch (IOException e) {
	    UtilsTestNG.failForException("Could not force delete on exit: "
		    + dir.getAbsolutePath(), new RuntimeException(e));
	}
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
	File dir = getUniquelyNamedFileWithPrefix(string);
	if (dir.exists()) {
	    throw new RuntimeException(new FileExistsException());
	}
	createDirectory(dir);
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
	AssertJUnit.assertTrue("Failed to create directory: " + child,
		child.mkdir());
	child.deleteOnExit();
	return child;
    }

    public static File createFileInParent(File parent, String fileName) {
	File child = new File(parent, fileName);
	try {
	    child.createNewFile();
	} catch (IOException e) {
	    e.printStackTrace();
	    UtilsTestNG.failForException("Could not create file: " + child, e);
	}
	child.deleteOnExit();
	return child;
    }

    /**
     * @return a new file having same contents (but different path) as the
     *         specified file.
     */
    public static File createTestFileWithContentsOfFile(File file) {
	File newFile = createTestFile();
	try {
	    FileUtils.copyFile(file, newFile);
	} catch (IOException e) {
	    UtilsTestNG.failForException(
		    "Couldn't create file with contents of " + file.toString(),
		    e);
	}
	newFile.deleteOnExit();
	return newFile;
    }

    /**
     * @return wether or not the directory has files in it.
     */
    public static boolean isDirectoryEmpty(File directory) {
	assertTrue(directory.isDirectory());
	File[] listFiles = directory.listFiles();
	return listFiles.length == 0;
    }
}
