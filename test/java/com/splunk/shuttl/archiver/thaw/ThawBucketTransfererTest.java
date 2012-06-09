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
package com.splunk.shuttl.archiver.thaw;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class ThawBucketTransfererTest {

	ThawBucketTransferer bucketTransferer;
	Bucket bucket;
	ArchiveFileSystem archiveFileSystem;
	ThawLocationProvider thawLocationProvider;
	BucketFactory bucketFactory;

	@BeforeMethod
	public void setUp() {
		bucket = TUtilsBucket.createBucket();
		thawLocationProvider = mock(ThawLocationProvider.class);
		archiveFileSystem = mock(ArchiveFileSystem.class);
		bucketFactory = mock(BucketFactory.class);
		bucketTransferer = new ThawBucketTransferer(thawLocationProvider,
				archiveFileSystem, bucketFactory);
	}

	@Test(groups = { "fast-unit" })
	public void _givenBucket_transferBucketFromArchiveToPathWhereParentIsThawLocation()
			throws IOException {
		File bucketLocation = createDirectory();
		when(thawLocationProvider.getLocationInThawForBucket(bucket)).thenReturn(
				bucketLocation);
		bucketTransferer.transferBucketToThaw(bucket);

		verify(archiveFileSystem).getFile(bucketLocation, bucket.getURI());
	}

	public void _whenArchiveFileSystemThrowsIOException_keepThrowing()
			throws IOException {
		doThrow(IOException.class).when(archiveFileSystem).getFile(any(File.class),
				eq(bucket.getURI()));
		try {
			bucketTransferer.transferBucketToThaw(bucket);
			fail();
		} catch (IOException e) {
			// We should come here instead of the fail call.
		}
	}

	public void _givenSuccessfulTransfer_returnBucketTransferedBucketOnLocalDisk()
			throws IOException {
		Bucket bucketToTransfer = TUtilsBucket.createBucket();
		File bucketLocationOnLocalDisk = createDirectory();
		Bucket bucketOnLocalDisk = mock(Bucket.class);

		when(thawLocationProvider.getLocationInThawForBucket(bucketToTransfer))
				.thenReturn(bucketLocationOnLocalDisk);
		when(
				bucketFactory.createWithIndexDirectoryAndSize(
						bucketToTransfer.getIndex(), bucketLocationOnLocalDisk,
						bucketToTransfer.getSize())).thenReturn(bucketOnLocalDisk);

		Bucket actualBucket = bucketTransferer
				.transferBucketToThaw(bucketToTransfer);
		assertEquals(bucketOnLocalDisk, actualBucket);
	}
}
