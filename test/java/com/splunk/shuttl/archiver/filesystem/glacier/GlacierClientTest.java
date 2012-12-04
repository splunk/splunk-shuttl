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
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import org.mockito.InOrder;
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

	public void download_givenArchiveIdWithKey_downloadsArchiveToFile() {
		String path = "/some/key/";
		String archiveId = "archiveId";
		glacierClient.putArchiveId(path, archiveId);
		File file = createFile();

		glacierClient.downloadArchiveToFile(path, file);
		verify(transferManager).download(vault, archiveId, file);
	}

	@Test(expectedExceptions = { IllegalArgumentException.class })
	public void download_givenExistingDir_throws() {
		glacierClient.downloadArchiveToFile(null, createDirectory());
	}

	public void download_givenNotExistingFile_makesDirsUpToFile() {
		String key = "/some/key";
		glacierClient.putArchiveId(key, "foo");

		File file = mock(File.class);
		when(file.exists()).thenReturn(false);

		glacierClient.downloadArchiveToFile(key, file);
		InOrder inOrder = inOrder(file);
		inOrder.verify(file).mkdirs();
		inOrder.verify(file).delete();
	}

	public void download_givenExistingFile_deletesFile() {
		String key = "/some/key";
		glacierClient.putArchiveId(key, "foo");

		File file = mock(File.class);
		when(file.exists()).thenReturn(true);

		glacierClient.downloadArchiveToFile(key, file);
		verify(file).delete();
		verify(file, never()).mkdirs();
	}
}
