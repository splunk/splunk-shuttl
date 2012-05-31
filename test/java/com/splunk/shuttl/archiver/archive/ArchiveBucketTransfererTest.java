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

package com.splunk.shuttl.archiver.archive;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.bucketsize.ArchiveBucketSize;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.fileSystem.FileOverwriteException;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class ArchiveBucketTransfererTest {

	private ArchiveFileSystem archive;
	private PathResolver pathResolver;
	private ArchiveBucketTransferer archiveBucketTransferer;
	private ArchiveBucketSize archiveBucketSize;

	@BeforeMethod
	public void setUp() {
		archive = mock(ArchiveFileSystem.class);
		pathResolver = mock(PathResolver.class);
		archiveBucketSize = mock(ArchiveBucketSize.class);
		archiveBucketTransferer = new ArchiveBucketTransferer(archive,
				pathResolver, archiveBucketSize);
	}

	@Test(groups = { "fast-unit" })
	public void transferBucketToArchive_givenValidBucketAndUri_putBucketWithArchiveFileSystem()
			throws IOException {
		Bucket bucket = TUtilsBucket.createBucket();
		URI destination = URI.create("file:/some/path");
		when(pathResolver.resolveArchivePath(bucket)).thenReturn(destination);
		archiveBucketTransferer.transferBucketToArchive(bucket);
		verify(archive).putFileAtomically(bucket.getDirectory(), destination);
	}

	public void transferBucketToArchive_archiveFileSystemThrowsIOException_throwRuntimeException()
			throws IOException {
		doThrow(IOException.class).when(archive).putFileAtomically(any(File.class),
				any(URI.class));
		RuntimeException exception = null;
		try {
			archiveBucketTransferer.transferBucketToArchive(mock(Bucket.class));
			fail();
		} catch (RuntimeException e) {
			exception = e;
		}
		assertNotNull(exception);
	}

	public void transferBucketToArchive_givenSuccessfulBucketTransfer_putBucketSizeInArchive() {
		Bucket bucket = mock(Bucket.class);
		archiveBucketTransferer.transferBucketToArchive(bucket);
		verify(archiveBucketSize).putSize(bucket);
	}

	public void transferBucketToArchive_whenBucketTransferIsUnsuccessful_dontPutBucketSizeInArchive()
			throws FileNotFoundException, FileOverwriteException, IOException {
		doThrow(Exception.class).when(archive).putFileAtomically(any(File.class),
				any(URI.class));
		try {
			archiveBucketTransferer.transferBucketToArchive(mock(Bucket.class));
			fail();
		} catch (Exception e) {
			// expected
		}
		verifyZeroInteractions(archiveBucketSize);
	}
}
