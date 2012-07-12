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
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class TUtilsFileTest {

	@Test(groups = { "fast-unit" })
	public void createTempDirectory_tenTwo_uniqueAndNotNull() {
		int times = 2;
		Set<String> absolutePaths = new HashSet<String>();
		for (int i = 0; i < times; i++) {
			String absolutePath = TUtilsFile.createDirectory().getAbsolutePath();
			assertNotNull(absolutePath);
			absolutePaths.add(absolutePath);
		}
		assertEquals(times, absolutePaths.size());
	}

	public void createTempDirectory_containNameOfThisTestClass_whenCalled() {
		File tempDir = TUtilsFile.createDirectory();
		String dirName = tempDir.getName();
		assertTrue(dirName.contains(getClass().getSimpleName()));
	}

	public void createNamedTempDirectory_fileDoesNotExist_getsCreated() {
		File dir = TUtilsFile.createPrefixedDirectory("NameOfTheDirectory");
		assertTrue(dir.exists());
	}

	public void createNamedTempDirectory_containsNameOfThisClass_toProvideUniquenessToTheDirectory() {
		File dir = TUtilsFile.createPrefixedDirectory("someName");
		String dirName = dir.getName();
		assertTrue(dirName.contains(getClass().getSimpleName()));
	}

	public void createNamedTempDirectory_withFileAsParentParameter_createsTheDirectoryInParent()
			throws IOException {
		File parent = TUtilsFile.createPrefixedDirectory("parent");
		File child = TUtilsFile.createDirectoryInParent(parent, "child");
		assertEquals(parent.listFiles()[0], child);
		File childsChild = TUtilsFile.createDirectoryInParent(child, "childsChild");
		assertEquals(child.listFiles()[0], childsChild);

		// Teardown
		FileUtils.deleteDirectory(parent);
		assertTrue(!childsChild.exists());
		assertTrue(!child.exists());
		assertTrue(!parent.exists());
	}

	public void createTestFileWithContentsOfFile_validInput_diffrentPaths() {
		File file = TUtilsFile.createFileWithRandomContent();
		File newFile = TUtilsFile.createFileWithContentsOfFile(file);
		AssertJUnit.assertFalse(file.getPath().equals(newFile.getPath()));
	}

	public void createTestFileWithContentsOfFile_validInput_sameContent() {
		File file = TUtilsFile.createFileWithRandomContent();
		File newFile = TUtilsFile.createFileWithContentsOfFile(file);
		TUtilsTestNG.assertFileContentsEqual(file, newFile);
	}

	public void createFileInParent_givenNameOfFile_createFileInParent()
			throws IOException {
		String childFileName = "child";
		File parent = TUtilsFile.createDirectory();
		File child = TUtilsFile.createFileInParent(parent, childFileName);
		assertEquals(parent, child.getParentFile());
		assertEquals(childFileName, child.getName());

		// Teardown
		FileUtils.deleteDirectory(parent);
	}

	public void createTmpDirectoryWithName_givenAName_createAnExistingDirectoryWithSPecifiedName() {
		File file = TUtilsFile
				.createDirectoryWithName("this is the name of the file");

		assertTrue(file.exists());
		assertTrue(file.isDirectory());
		assertEquals("this is the name of the file", file.getName());
	}

	public void isDirectoryEmpty_givenNewTempDirectory_empty() {
		File tempDirectory = TUtilsFile.createDirectory();
		assertTrue(TUtilsFile.isDirectoryEmpty(tempDirectory));
	}

	public void isDirectoryEmpty_givenDirectoryWithAChildDirectory_notEmpty()
			throws IOException {
		File dirWithChildDir = TUtilsFile.createDirectory();
		try {
			TUtilsFile.createDirectoryInParent(dirWithChildDir, "childDir");
			assertTrue(!TUtilsFile.isDirectoryEmpty(dirWithChildDir));
		} finally {
			FileUtils.deleteDirectory(dirWithChildDir);
		}
	}

	public void isDirectoryEmpty_givenDirectoryWithAChildFile_notEmpty()
			throws IOException {
		File dirWithChildFile = TUtilsFile.createDirectory();
		try {
			TUtilsFile.createFileInParent(dirWithChildFile, "childFile");
			assertTrue(!TUtilsFile.isDirectoryEmpty(dirWithChildFile));
		} finally {
			FileUtils.deleteDirectory(dirWithChildFile);
		}
	}

	public void createTestFileWithName_givenName_createsFileWithName() {
		File namedFile = TUtilsFile.createTestFileWithName("name");
		assertEquals("name", namedFile.getName());
	}

	public void getShuttlTestDirectory_nothing_isDefinedToLiveInVarTmp() {
		File varTmp = new File(File.separator + "var" + File.separator + "tmp");
		File shuttlTestDirectory = TUtilsFile.getShuttlTestDirectory();
		assertEquals(varTmp.getAbsolutePath(), shuttlTestDirectory.getParentFile()
				.getAbsolutePath());
	}

	public void getShuttlTestDirectory_nothing_exists() {
		assertTrue(TUtilsFile.getShuttlTestDirectory().exists());
	}

	public void getShuttlTestDirectory_callTwice_existsWithNoErrors() {
		assertTrue(TUtilsFile.getShuttlTestDirectory().exists());
		assertTrue(TUtilsFile.getShuttlTestDirectory().exists());
	}

	public void createDirectory_nothing_livesInsideShuttlTestDirectory() {
		File dir = TUtilsFile.createDirectory();
		File shuttlTestDir = TUtilsFile.getShuttlTestDirectory();
		assertEquals(shuttlTestDir.getAbsolutePath(), dir.getParentFile()
				.getAbsolutePath());
	}

	public void createFile_nothing_livesInsideShuttlTestDirectory() {
		File file = TUtilsFile.createFile();
		File shuttlTestDir = TUtilsFile.getShuttlTestDirectory();
		assertEquals(shuttlTestDir.getAbsolutePath(), file.getParentFile()
				.getAbsolutePath());
	}
}
