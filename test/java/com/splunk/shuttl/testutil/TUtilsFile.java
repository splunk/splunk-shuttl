// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.testutil;

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
public class TUtilsFile {

	/**
	 * @return a temporary file with random content.
	 * 
	 * @see TUtilsFile#createFile()
	 * @see TUtilsFile#createFileWithRandomContent()
	 */
	public static File createFileWithRandomContent() {
		File testFile = createFile();
		populateFileWithRandomContent(testFile);
		return testFile;
	}

	/**
	 * @return a temporary, existing, empty file that will be deleted when the VM
	 *         terminates.
	 * 
	 * @see TUtilsFile#createFilePath()
	 */
	public static File createFile() {
		File testFile = null;
		try {
			testFile = File.createTempFile(TUtilsFile.class.getSimpleName(), "");
			testFile.deleteOnExit();
		} catch (IOException e) {
			TUtilsTestNG.failForException("Couldn't create a test file.", e);
		}
		return testFile;
	}

	/**
	 * @return a temporary file path. The returned file doesn't exist, but the the
	 *         path to the file was valid at the time of this method call.
	 * 
	 * @see TUtilsFile#createFile()
	 */
	public static File createFilePath() {
		File testFile = createFile();
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
			TUtilsTestNG.failForException("Failed to write random content", e);
		} finally {
			IOUtils.closeQuietly(printStream);
		}
	}

	/**
	 * Creates a temporary directory with a unique name. <br/>
	 * The name of the directory includes the file class name of the caller to the
	 * method, so it's easier to track leakage if it some how persists between
	 * runs.
	 * 
	 * @return temporary directory that's deleted when the VM terminates.
	 */
	public static File createDirectory() {
		// TODO: There's a race condition here - the directory might be
		// created between the calls to getUniquelyNamedFile() and
		// createDirectory().
		File dir = getUniquelyNamedFile();
		createDirectory(dir);
		return dir;
	}

	/**
	 * Creates a directory by first invoking createTempDirectory() and then
	 * creating a new directory inside that with specified name. This is done the
	 * prevent collisions when same name is used.
	 * 
	 * @param name
	 *          The name of the directory to be created.
	 * @return A directory with specified name that will be removed on JVM exit
	 */
	public static File createDirectoryWithName(String name) {
		File tmpDirectory = new File(createDirectory(), name);
		createDirectory(tmpDirectory);
		try {
			FileUtils.forceDeleteOnExit(tmpDirectory);
		} catch (IOException e) {
			TUtilsTestNG.failForException("Could not force delete tmpDirectory: "
					+ tmpDirectory, e);
		}
		return tmpDirectory;
	}

	private static File getUniquelyNamedFile() {
		return getUniquelyNamedFileWithPrefix("test-dir");
	}

	private static File getUniquelyNamedFileWithPrefix(String prefix) {
		Class<?> callerToThisMethod = MethodCallerHelper.getCallerToMyMethod();
		String tmpRoot = System.getProperty("java.io.tmpdir");
		String tempDirName = tmpRoot + File.separator + prefix + "-"
				+ callerToThisMethod.getSimpleName();
		File dir = new File(tempDirName);
		while (dir.exists()) {
			tempDirName += "-" + RandomStringUtils.randomAlphanumeric(2);
			dir = new File(tempDirName);
		}
		return dir;
	}

	private static void createDirectory(File dir) {
		if (!dir.mkdir())
			TUtilsTestNG.failForException(
					"Could not create directory: " + dir.getAbsolutePath(),
					new RuntimeException());
		try {
			FileUtils.forceDeleteOnExit(dir);
		} catch (IOException e) {
			TUtilsTestNG.failForException(
					"Could not force delete on exit: " + dir.getAbsolutePath(),
					new RuntimeException(e));
		}
	}

	/**
	 * Creates a temp directory with a name of the parameter. <br/>
	 * Wraps the FileExistException in a {@link RuntimeException} to avoid the
	 * try/catch.
	 * 
	 * @param string
	 *          name of the temp directory
	 * @return temporary directory that's deleted when the VM terminates.
	 */
	public static File createPrefixedDirectory(String string) {
		File dir = getUniquelyNamedFileWithPrefix(string);
		if (dir.exists())
			throw new RuntimeException(new FileExistsException());
		createDirectory(dir);
		return dir;
	}

	/**
	 * Creates directory in the parent with the name of the String. <br/>
	 * Note: This file is not temporary and must be removed.
	 * 
	 * @param parent
	 *          where the child directory will live.
	 * @param string
	 *          name of the child
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
			TUtilsTestNG.failForException("Could not create file: " + child, e);
		}
		child.deleteOnExit();
		return child;
	}

	/**
	 * @return a new file having same contents (but different path) as the
	 *         specified file.
	 */
	public static File createFileWithContentsOfFile(File file) {
		File newFile = createFile();
		try {
			FileUtils.copyFile(file, newFile);
		} catch (IOException e) {
			TUtilsTestNG.failForException("Couldn't create file with contents of "
					+ file.toString(), e);
		}
		newFile.deleteOnExit();
		return newFile;
	}

	/**
	 * @return whether or not the directory has files in it.
	 */
	public static boolean isDirectoryEmpty(File directory) {
		assertTrue(directory.isDirectory());
		File[] listFiles = directory.listFiles();
		return listFiles.length == 0;
	}

	/**
	 * @return a file named a certain way.
	 */
	public static File createTestFileWithName(String name) {
		return TUtilsFile.createFileInParent(FileUtils.getTempDirectory(), name);
	}
}
