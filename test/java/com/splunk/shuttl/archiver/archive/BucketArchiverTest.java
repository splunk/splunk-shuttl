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

import static org.mockito.Mockito.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.importexport.BucketExporter;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketArchiverTest {

	private BucketArchiver bucketArchiver;
	private BucketExporter exporter;
	private ArchiveBucketTransferer archiveBucketTransferer;
	private BucketDeleter deletesBuckets;

	private Bucket bucket;

	@BeforeMethod(groups = { "fast-unit" })
	public void setUp() {
		exporter = mock(BucketExporter.class);
		archiveBucketTransferer = mock(ArchiveBucketTransferer.class);
		deletesBuckets = mock(BucketDeleter.class);
		bucketArchiver = new BucketArchiver(exporter, archiveBucketTransferer,
				deletesBuckets);

		bucket = TUtilsBucket.createBucket();
	}

	@Test(groups = { "fast-unit" })
	public void archiveBucket_givenBucket_exportsBucketAndTransfersBucket() {
		Bucket exportedBucket = mock(Bucket.class);
		when(exporter.exportBucket(bucket)).thenReturn(exportedBucket);
		bucketArchiver.archiveBucket(bucket);
		verify(archiveBucketTransferer).transferBucketToArchive(exportedBucket);
	}

	public void archiveBucket_givenBucketAndExportedBucket_deletesBothBucketsWithBucketDeleter() {
		Bucket exportedBucket = mock(Bucket.class);
		when(exporter.exportBucket(bucket)).thenReturn(exportedBucket);
		bucketArchiver.archiveBucket(bucket);
		verify(deletesBuckets).deleteBucket(bucket);
		verify(deletesBuckets).deleteBucket(exportedBucket);
	}

	public void archiveBucket_whenExceptionIsThrown_deleteExportedBucketButNotOriginalBucket() {
		Bucket exportedBucket = TUtilsBucket.createBucket();
		when(exporter.exportBucket(bucket)).thenReturn(exportedBucket);
		doThrow(new RuntimeException()).when(archiveBucketTransferer)
				.transferBucketToArchive(exportedBucket);
		try {
			bucketArchiver.archiveBucket(bucket);
		} catch (Throwable e) {
			// Do nothing.
		}
		verify(deletesBuckets).deleteBucket(exportedBucket);
		verifyNoMoreInteractions(deletesBuckets);
	}

	public void archiveBucket_whenExportBucketIsSameAsOriginalBucket_deleteBucketTwice() {
		when(exporter.exportBucket(bucket)).thenReturn(bucket);
		bucketArchiver.archiveBucket(bucket);
		verify(deletesBuckets, times(2)).deleteBucket(bucket);
	}
}
