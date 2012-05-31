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
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.model.Bucket;

@Test(groups = { "fast-unit" })
public class ArchivedBucketSizeTest {

	private ArchivedBucketsSize archivedBucketsSize;
	private PathResolver pathResolver;
	private ArchiveFileSystem archiveFileSystem;
	private BucketSizeFile bucketSizeFile;
	private BucketSizeFilePathResolver bucketSizeFilePathResolver;

	@BeforeMethod
	public void setUp() {
		pathResolver = mock(PathResolver.class);
		archiveFileSystem = mock(ArchiveFileSystem.class);
		bucketSizeFile = mock(BucketSizeFile.class);
		bucketSizeFilePathResolver = mock(BucketSizeFilePathResolver.class);
		archivedBucketsSize = new ArchivedBucketsSize(pathResolver, bucketSizeFile,
				archiveFileSystem, bucketSizeFilePathResolver);
	}

	public void putSize_givenBucketSizeFile_getsFileWithBucketSize() {
		Bucket bucket = mock(Bucket.class);
		archivedBucketsSize.putSize(bucket);
		verify(bucketSizeFile).getFileWithBucketSize(bucket);
	}

	public void putSize_givenPathResolver_getsMetadataFolderForBucket() {
		Bucket bucket = mock(Bucket.class);
		archivedBucketsSize.putSize(bucket);
		verify(pathResolver).getMetadataFolderForBucket(bucket);
	}

	public void putSize_givenMetadataFolderAndFileWithBucketSize_resolvesPathOnArchiveFileSystem() {
		Bucket bucket = mock(Bucket.class);
		File fileWithBucketSize = createFile();
		when(bucketSizeFile.getFileWithBucketSize(bucket)).thenReturn(
				fileWithBucketSize);
		URI metadataFolderUri = URI.create("uri:/to/metadata/folder");
		when(pathResolver.getMetadataFolderForBucket(bucket)).thenReturn(
				metadataFolderUri);
		archivedBucketsSize.putSize(bucket);
		verify(bucketSizeFilePathResolver).resolveBucketSizeFilePath(
				fileWithBucketSize, metadataFolderUri);

	}

	public void putSize_givenFileWithBucketSizeAndPathOnArchiveFileSystem_transfersFileWithSizeToArchiveFileSystem()
			throws IOException {
		Bucket bucket = mock(Bucket.class);
		File fileWithBucketsSize = createFile();
		when(bucketSizeFile.getFileWithBucketSize(bucket)).thenReturn(
				fileWithBucketsSize);
		URI pathOnArchiveFileSystem = URI.create("path:/on/archive/file/system");
		when(
				bucketSizeFilePathResolver.resolveBucketSizeFilePath(
						eq(fileWithBucketsSize), any(URI.class))).thenReturn(
				pathOnArchiveFileSystem);
		archivedBucketsSize.putSize(bucket);
		verify(archiveFileSystem).putFileAtomically(fileWithBucketsSize,
				pathOnArchiveFileSystem);
	}
}
