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

	private LocalFileSystemConstants localFileSystemConstants;

	@BeforeMethod
	public void setUp() {
		localFileSystemConstants = new LocalFileSystemConstants();
		removeArchiverDirectory();
	}

	private void removeArchiverDirectory() {
		File directory = localFileSystemConstants.getArchiverDirectory();
		FileUtils.deleteQuietly(directory);
	}

	@AfterMethod
	public void tearDown() {
		removeArchiverDirectory();
	}

	@Test(groups = { "fast-unit" })
	public void getArchiverDirectory_doesNotExist_dirExists() {
		assertDoesNotExist(localFileSystemConstants.ARCHIVER_DIRECTORY_PATH);
		File dir = localFileSystemConstants.getArchiverDirectory();
		assertTrue(dir.exists());
	}

	private void assertDoesNotExist(String path) {
		assertFalse(new File(path).exists());
	}

	public void getSafeLocation_doesNotExist_dirExistsInsideArchiverDirectory() {
		assertDoesNotExist(localFileSystemConstants.SAFE_PATH);
		File safeDir = localFileSystemConstants.getSafeDirectory();
		assertExistsInsideArchiverDirectory(safeDir);
	}

	private void assertParentIsArchiverDirectory(File safeDir) {
		assertEquals(localFileSystemConstants.getArchiverDirectory(),
				safeDir.getParentFile());
	}

	public void getFailLocation_doesNotExist_dirExistsInsideArchiverDirectory() {
		assertDoesNotExist(localFileSystemConstants.FAIL_PATH);
		File failLocation = localFileSystemConstants.getFailDirectory();
		assertExistsInsideArchiverDirectory(failLocation);
	}

	private void assertExistsInsideArchiverDirectory(File failLocation) {
		assertTrue(failLocation.exists());
		assertParentIsArchiverDirectory(failLocation);
	}

	public void getArchiveLocksDirectory_doesNotExist_dirExistsInsideArchiverDirectory() {
		assertDoesNotExist(localFileSystemConstants.ARCHIVE_LOCKS_PATH);
		assertExistsInsideArchiverDirectory(localFileSystemConstants
				.getArchiveLocksDirectory());
	}

	public void getCsvDirectory_doesNotExist_dirExistsInsideArchiverDirectory() {
		assertDoesNotExist(localFileSystemConstants.CSV_PATH);
		assertExistsInsideArchiverDirectory(localFileSystemConstants
				.getCsvDirectory());
	}

	public void getThawLocksDirectory_doesNotExist_dirExistsInsideArchiverDirectory() {
		assertDoesNotExist(localFileSystemConstants.THAW_LOCKS_PATH);
		assertExistsInsideArchiverDirectory(localFileSystemConstants
				.getThawLocksDirectory());
	}

	public void getThawTransferDirectory_doesNotExist_dirExistsInsideArchiverDirectory() {
		assertDoesNotExist(localFileSystemConstants.THAW_TRANSFERS_PATH);
		assertExistsInsideArchiverDirectory(localFileSystemConstants
				.getThawTransfersDirectory());
	}
}
