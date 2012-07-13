// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.splunk.shuttl.archiver;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class LocalFileSystemPathsTest {

	private LocalFileSystemPaths localFileSystemPaths;
	private String testDirectoryPath;

	@BeforeMethod
	public void setUp() {
		testDirectoryPath = createFilePath().getAbsolutePath();
		localFileSystemPaths = new LocalFileSystemPaths(testDirectoryPath);
		removeArchiverDirectory();
	}

	private void removeArchiverDirectory() {
		File directory = localFileSystemPaths.getArchiverDirectory();
		FileUtils.deleteQuietly(directory);
	}

	@AfterMethod
	public void tearDown() {
		removeArchiverDirectory();
	}

	@Test(groups = { "fast-unit" })
	public void getArchiverDirectory_givenTestDirectory_dirIsChildToTestDirectory() {
		assertDoesNotExist(testDirectoryPath);
		File dir = localFileSystemPaths.getArchiverDirectory();
		assertEquals(testDirectoryPath, dir.getParent());
	}

	private void assertDoesNotExist(String path) {
		assertFalse(new File(path).exists());
	}

	public void getSafeLocation_setUp_dirExistsInsideArchiverDirectory() {
		File safeDir = localFileSystemPaths.getSafeDirectory();
		assertExistsInsideArchiverDirectory(safeDir);
	}

	private void assertExistsInsideArchiverDirectory(File failLocation) {
		assertTrue(failLocation.exists());
		assertParentIsArchiverDirectory(failLocation);
	}

	private void assertParentIsArchiverDirectory(File safeDir) {
		assertEquals(localFileSystemPaths.getArchiverDirectory(),
				safeDir.getParentFile());
	}

	public void getFailLocation_setUp_dirExistsInsideArchiverDirectory() {
		File failLocation = localFileSystemPaths.getFailDirectory();
		assertExistsInsideArchiverDirectory(failLocation);
	}

	public void getArchiveLocksDirectory_setUp_dirExistsInsideArchiverDirectory() {
		assertExistsInsideArchiverDirectory(localFileSystemPaths
				.getArchiveLocksDirectory());
	}

	public void getCsvDirectory_setUp_dirExistsInsideArchiverDirectory() {
		assertExistsInsideArchiverDirectory(localFileSystemPaths
				.getCsvDirectory());
	}

	public void getThawLocksDirectory_setUp_dirExistsInsideArchiverDirectory() {
		assertExistsInsideArchiverDirectory(localFileSystemPaths
				.getThawLocksDirectory());
	}

	public void getThawTransferDirectory_setUp_dirExistsInsideArchiverDirectory() {
		assertExistsInsideArchiverDirectory(localFileSystemPaths
				.getThawTransfersDirectory());
	}

	public void getArchiverDirectory_givenTildeWithoutRoot_resolvesTildeAsUserHome() {
		File archiverDirectory = new LocalFileSystemPaths("~/archiver_directory")
				.getArchiverDirectory();
		File expected = new File(FileUtils.getUserDirectoryPath(),
				"archiver_directory");
		assertEquals(expected.getAbsolutePath(), archiverDirectory.getParentFile()
				.getAbsolutePath());
	}

	public void getArchiverDirectory_givenUriWithFileSchemeAndTilde_resolvesTildeAsUserHome() {
		File archiverDirectory = new LocalFileSystemPaths("file:/~/archiver_dir")
				.getArchiverDirectory();
		File expected = new File(FileUtils.getUserDirectoryPath(), "archiver_dir");
		assertEquals(expected.getAbsolutePath(), archiverDirectory.getParentFile()
				.getAbsolutePath());
	}

	public void getMetadataDirectory_setUp_dirExistsInsideArchiverDirectory() {
		assertExistsInsideArchiverDirectory(localFileSystemPaths
				.getMetadataDirectory());
	}

	@Test(expectedExceptions = { ArchiverMBeanNotRegisteredException.class })
	public void create_withNoArchiverMBeanRegistration_throwsRuntimeException() {
		LocalFileSystemPaths.create();
	}
}
