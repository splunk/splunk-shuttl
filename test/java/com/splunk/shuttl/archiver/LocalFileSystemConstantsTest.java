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

import static org.testng.AssertJUnit.*;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class LocalFileSystemConstantsTest {

	@BeforeMethod
	public void setUp() {
		removeArchiverDirectory();
	}

	private void removeArchiverDirectory() {
		File directory = LocalFileSystemConstants.getArchiverDirectory();
		FileUtils.deleteQuietly(directory);
	}

	@AfterMethod
	public void tearDown() {
		removeArchiverDirectory();
	}

	@Test(groups = { "fast-unit" })
	public void getArchiverDirectory_doesNotExist_dirExists() {
		assertDoesNotExist(LocalFileSystemConstants.ARCHIVER_DIRECTORY_PATH);
		File dir = LocalFileSystemConstants.getArchiverDirectory();
		assertTrue(dir.exists());
	}

	private void assertDoesNotExist(String path) {
		assertFalse(new File(path).exists());
	}

	public void getSafeLocation_doesNotExist_dirExistsInsideArchiverDirectory() {
		assertDoesNotExist(LocalFileSystemConstants.SAFE_PATH);
		File safeDir = LocalFileSystemConstants.getSafeDirectory();
		assertExistsInsideArchiverDirectory(safeDir);
	}

	private void assertParentIsArchiverDirectory(File safeDir) {
		assertEquals(LocalFileSystemConstants.getArchiverDirectory(),
				safeDir.getParentFile());
	}

	public void getFailLocation_doesNotExist_dirExistsInsideArchiverDirectory() {
		assertDoesNotExist(LocalFileSystemConstants.FAIL_PATH);
		File failLocation = LocalFileSystemConstants.getFailDirectory();
		assertExistsInsideArchiverDirectory(failLocation);
	}

	private void assertExistsInsideArchiverDirectory(File failLocation) {
		assertTrue(failLocation.exists());
		assertParentIsArchiverDirectory(failLocation);
	}

	public void getLocksDirectory_doesNotExist_dirExistsInsideArchiverDirectory() {
		assertDoesNotExist(LocalFileSystemConstants.LOCKS_PATH);
		assertExistsInsideArchiverDirectory(LocalFileSystemConstants
				.getLocksDirectory());
	}

	public void getCsvDirectory_doesNotExist_dirExistsInsideArchiverDirectory() {
		assertDoesNotExist(LocalFileSystemConstants.CSV_PATH);
		assertExistsInsideArchiverDirectory(LocalFileSystemConstants
				.getCsvDirectory());
	}
}
