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

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.testutil.HadoopFileSystemPutter.LocalFileNotFound;

@Test(groups = { "fast-unit" })
public class HadoopFileSystemPutterTest {

	private HadoopFileSystemPutter putter;
	private FileSystem fileSystem;

	@BeforeMethod(groups = { "fast-unit" })
	public void setUp() {
		fileSystem = TUtilsFileSystem.getLocalFileSystem();
		putter = HadoopFileSystemPutter.create(fileSystem);
	}

	@AfterMethod(groups = { "fast-unit" })
	public void tearDown() {
		putter.deleteMyFiles();
	}

	@Test(groups = { "fast-unit" })
	public void createdTempFile_should_exist() {
		File tempFile = createFile();
		assertTrue(tempFile.exists());
	}

	@Test(groups = { "fast-unit" })
	public void copyingFileThatExists_should_existInFileSystemCopiedTo()
			throws IOException {
		File tempFile = createFile();
		putter.putFile(tempFile);
		assertTrue(putter.isFileCopiedToFileSystem(tempFile));
	}

	@Test(groups = { "fast-unit" }, expectedExceptions = LocalFileNotFound.class)
	public void copyingFileThatDoesntExist_should_throw_LocalFileNotFound() {
		File nonExistingFile = new File("file-does-not-exist");
		putter.putFile(nonExistingFile);
	}

	@Test(groups = { "fast-unit" })
	public void fileThatIsNotCopied_shouldNot_existInFileSystem() {
		assertFalse(putter.isFileCopiedToFileSystem(new File("somefile")));
	}

	@Test(groups = { "fast-unit" })
	public void should_bePossibleToGetTheDirectory_where_allThisTestCasesFilesAreStored() {
		assertNotNull(putter.getPathOfMyFiles());
	}

	@Test(groups = { "fast-unit" })
	public void pathWhereAClassesFilesAreStored_should_differForDifferentClasses() {
		ClassA classA = new ClassA();
		ClassB classB = new ClassB();
		boolean isDifferentClassses = !classA.getClass().getName()
				.equals(classB.getClass().getName());
		assertTrue(isDifferentClassses);

		Path classAStoragePath = classA.getPathWhereFilesAreStored();
		Path classBStoragePath = classB.getPathWhereFilesAreStored();
		assertTrue(!classAStoragePath.equals(classBStoragePath));
	}

	private class ClassA {

		public Path getPathWhereFilesAreStored() {
			return putter.getPathOfMyFiles();
		}
	}

	private class ClassB {
		public Path getPathWhereFilesAreStored() {
			return putter.getPathOfMyFiles();
		}
	}

	@Test(groups = { "fast-unit" })
	public void after_putFile_then_deleteMyFiles_should_removeTheDirectory_where_thisClassPutFilesOnTheFileSystem()
			throws IOException {
		Path myFiles = putter.getPathOfMyFiles();
		putter.putFile(createFile());
		assertTrue(fileSystem.exists(myFiles));
		putter.deleteMyFiles();
		assertFalse(fileSystem.exists(myFiles));
	}

	@Test(groups = { "fast-unit" })
	public void should_beAbleToGetPath_where_fileIsPut() {
		assertNotNull(putter.getPathForFile(createFile()));
	}

	@Test(groups = { "fast-unit" })
	public void path_where_localFileIsPut_should_differForDifferentFiles() {
		File file1 = createFile();
		File file2 = createFile();
		assertTrue(!file1.getAbsolutePath().equals(file2.getAbsolutePath()));

		Path path1 = putter.getPathForFile(file1);
		Path path2 = putter.getPathForFile(file2);
		assertTrue(!path1.equals(path2));
	}

	@Test(groups = { "fast-unit" })
	public void should_bePossibleToGetPathToFile_with_fileName() {
		File file = createFile();
		Path expected = putter.getPathForFile(file);
		Path actual = putter.getPathForFileName(file.getName());

		assertEquals(actual, expected);
	}
}
