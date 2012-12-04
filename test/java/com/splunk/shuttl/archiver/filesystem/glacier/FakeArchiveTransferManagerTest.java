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
package com.splunk.shuttl.archiver.filesystem.glacier;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glacier.transfer.UploadResult;
import com.splunk.shuttl.archiver.filesystem.glacier.FakeArchiveTransferManager.UnknownArchiveIdException;
import com.splunk.shuttl.testutil.TUtilsTestNG;

@Test(groups = { "fast-unit" })
public class FakeArchiveTransferManagerTest {

	private File managedDirectory;
	private FakeArchiveTransferManager transferManager;

	@BeforeMethod
	public void setUp() {
		managedDirectory = createDirectory();
		transferManager = new FakeArchiveTransferManager(managedDirectory);
	}

	public void upload_givenFile_putsFileInItsDirectory()
			throws AmazonServiceException, AmazonClientException,
			FileNotFoundException {
		File fileWithContents = createFileWithRandomContent();

		transferManager.upload(null, null, fileWithContents);

		File fileInDirectory = getFileInManagedDirectory();
		TUtilsTestNG.assertFileContentsEqual(fileInDirectory, fileWithContents);
	}

	private File getFileInManagedDirectory() {
		return managedDirectory.listFiles()[0];
	}

	public void upload_givenFile_returnsUploadResultWithArchiveIdWhichIdentifiesTheFile()
			throws AmazonServiceException, AmazonClientException,
			FileNotFoundException {
		UploadResult uploadResult = transferManager
				.upload(null, null, createFile());

		File file = getFileInManagedDirectory();
		File fileByArchiveId = transferManager.getFile(uploadResult.getArchiveId());
		assertEquals(file.getAbsolutePath(), fileByArchiveId.getAbsolutePath());
	}

	public void download_givenUploadedFile_downloadsFileToSpecifiedOutputFile()
			throws AmazonServiceException, AmazonClientException,
			FileNotFoundException {
		File fileWithContent = createFileWithRandomContent();
		UploadResult upload = transferManager.upload(null, null, fileWithContent);

		File outputFile = createFilePath();
		transferManager.download(null, upload.getArchiveId(), outputFile);
		TUtilsTestNG.assertFileContentsEqual(outputFile, fileWithContent);
	}

	@Test(expectedExceptions = { RuntimeException.class })
	public void download_givenExistingFile_throws()
			throws AmazonServiceException, AmazonClientException,
			FileNotFoundException {
		UploadResult upload = transferManager.upload(null, null, createFile());
		transferManager.download(null, upload.getArchiveId(), createFile());
	}

	@Test(expectedExceptions = { UnknownArchiveIdException.class })
	public void download_nonExistingArchiveId_throws() {
		transferManager.download(null, "nonExistingArchiveId", createFilePath());
	}
}
