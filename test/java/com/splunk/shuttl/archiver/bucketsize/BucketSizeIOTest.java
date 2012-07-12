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
package com.splunk.shuttl.archiver.bucketsize;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketSizeIOTest {

	private BucketSizeIO bucketSizeIO;
	private ArchiveFileSystem archiveFileSystem;
	private File tempDir;
	private LocalFileSystemPaths localFileSystemPaths;
	private Bucket bucket;

	@BeforeMethod
	public void setUp() {
		archiveFileSystem = mock(ArchiveFileSystem.class);
		tempDir = createDirectory();
		localFileSystemPaths = new LocalFileSystemPaths(tempDir.getAbsolutePath());
		bucketSizeIO = new BucketSizeIO(archiveFileSystem, localFileSystemPaths);
		bucket = TUtilsBucket.createBucket();
	}

	@AfterMethod
	public void tearDown() {
		FileUtils.deleteQuietly(tempDir);
	}

	public void getFileWithBucketSize_givenBucket_returnsFileWithSizeOfBucket()
			throws IOException {
		File fileWithBucketSize = bucketSizeIO.getFileWithBucketSize(bucket);
		List<String> linesOfFile = FileUtils.readLines(fileWithBucketSize);
		assertEquals(1, linesOfFile.size());
		String firstLine = linesOfFile.get(0);
		assertEquals(bucket.getSize() + "", firstLine);
	}

	public void getFileWithBucketSize_givenBucket_fileNameContainsBucketNameForUniquness() {
		File fileWithBucketSize = bucketSizeIO.getFileWithBucketSize(bucket);
		assertTrue(fileWithBucketSize.getName().contains(bucket.getName()));
	}

	public void getFileWithBucketSize_givenBucket_fileNameContainsSizeLitteralForExternalUnderstandingOfTheFile() {
		File fileWithBucketSize = bucketSizeIO.getFileWithBucketSize(bucket);
		assertTrue(fileWithBucketSize.getName().contains(".size"));
	}

	public void getFileWithBucketSize_givenLocalFileSystemPaths_isInMetadataDirectory() {
		File file = bucketSizeIO.getFileWithBucketSize(bucket);
		assertEquals(localFileSystemPaths.getMetadataDirectory().getAbsolutePath(),
				file.getParentFile().getAbsolutePath());
	}

	public void readSizeFromRemoteFile_givenArchiveFileSystem_getsInputStreamToFileWithSize()
			throws IOException {
		File fileWithBucketSize = bucketSizeIO.getFileWithBucketSize(bucket);
		URI uriToFile = URI.create("uri:/to/remote/file/with/size");
		InputStream inputStreamToFile = new FileInputStream(fileWithBucketSize);
		when(archiveFileSystem.openFile(uriToFile)).thenReturn(inputStreamToFile);

		Long sizeFromRemoteFile = bucketSizeIO.readSizeFromRemoteFile(uriToFile);
		assertEquals(bucket.getSize(), sizeFromRemoteFile);
	}

	// TODO: Test closing stream for successful and unsuccessful reads.
}
