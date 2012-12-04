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
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;

@Test(groups = { "fast-unit" })
public class GlacierClientTest {

	private ArchiveTransferManager transferManager;
	private String vault;
	private GlacierClient glacierClient;
	private Map<String, String> archiveIds;

	@BeforeMethod
	public void setUp() {
		transferManager = mock(ArchiveTransferManager.class);
		vault = "vault";
		archiveIds = new HashMap<String, String>();
		glacierClient = new GlacierClient(transferManager, vault, archiveIds);
	}

	public void upload_givenPath_canGetArchiveIdWithPath()
			throws AmazonServiceException, AmazonClientException,
			FileNotFoundException {
		File file = createFile();
		String dst = "/path/dst";
		UploadResult uploadResult = mock(UploadResult.class);
		String archiveId = "archiveId";
		when(uploadResult.getArchiveId()).thenReturn(archiveId);
		when(transferManager.upload(vault, dst.toString(), file)).thenReturn(
				uploadResult);

		glacierClient.upload(file, dst);
		assertEquals(archiveId, glacierClient.getArchiveId(dst));
	}

	@Test(expectedExceptions = { GlacierArchiveIdDoesNotExist.class })
	public void getArchiveId_doesNotContainPath_throws() {
		glacierClient.getArchiveId("/path/doesNotExist");
	}

	public void download_givenArchiveIdWithPath_getsTheArchive() {
		String filename = "filename";
		String path = "/path/to/" + filename;
		String archiveId = "archiveId";
		glacierClient.putArchiveId(path, archiveId);
		File dir = createDirectory();

		glacierClient.downloadToDir(path, dir);
		verify(transferManager).download(eq(vault), eq(archiveId),
				eq(new File(dir, filename + ".csv")));
	}

	@Test(expectedExceptions = { IllegalArgumentException.class })
	public void download_givenExistingFileAndNotDir_throws() {
		glacierClient.downloadToDir(null, createFile());
	}

	public void download_givenNotExistingFile_makesDirs() {
		String path = "/path";
		glacierClient.putArchiveId(path, "foo");

		File dir = createDirectory();
		FileUtils.deleteQuietly(dir);
		assertFalse(dir.exists());
		glacierClient.downloadToDir(path, dir);
		assertTrue(dir.exists());
	}
}
