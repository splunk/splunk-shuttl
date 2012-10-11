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

import static java.util.Arrays.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.importexport.BucketExportController;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketArchiverTest {

	private BucketArchiver bucketArchiver;
	private BucketExportController exporter;
	private ArchiveBucketTransferer archiveBucketTransferer;
	private BucketDeleter deletesBuckets;

	private LocalBucket bucket;
	private List<BucketFormat> bucketFormats;

	@BeforeMethod(groups = { "fast-unit" })
	public void setUp() {
		exporter = mock(BucketExportController.class);
		archiveBucketTransferer = mock(ArchiveBucketTransferer.class);
		deletesBuckets = mock(BucketDeleter.class);
		bucketFormats = asList(BucketFormat.SPLUNK_BUCKET);
		bucketArchiver = new BucketArchiver(exporter, archiveBucketTransferer,
				deletesBuckets, bucketFormats);

		bucket = TUtilsBucket.createBucket();
	}

	@Test(groups = { "fast-unit" })
	public void archiveBucket_givenBucket_exportsBucketAndTransfersBucket() {
		LocalBucket exportedBucket = getMockedBucketReturnFromExporter();
		bucketArchiver.archiveBucket(bucket);
		verify(archiveBucketTransferer).transferBucketToArchive(exportedBucket);
	}

	private LocalBucket getMockedBucketReturnFromExporter() {
		LocalBucket exportedBucket = TUtilsBucket.createBucket();
		when(exporter.exportBucket(eq(bucket), any(BucketFormat.class)))
				.thenReturn(exportedBucket);
		return exportedBucket;
	}

	public void archiveBucket_givenBucketAndExportedBucket_deletesBothBucketsWithBucketDeleter() {
		LocalBucket exportedBucket = getMockedBucketReturnFromExporter();
		bucketArchiver.archiveBucket(bucket);
		verify(deletesBuckets).deleteBucket(bucket);
		verify(deletesBuckets).deleteBucket(exportedBucket);
	}

	public void archiveBucket_whenExceptionIsThrown_deleteExportedBucketButNotOriginalBucket() {
		LocalBucket exportedBucket = getMockedBucketReturnFromExporter();
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

	public void archiveBucket_whenExportBucketIsSameAsOriginalBucket_deleteBucketOnce() {
		when(exporter.exportBucket(eq(bucket), any(BucketFormat.class)))
				.thenReturn(bucket);
		bucketArchiver.archiveBucket(bucket);
		verify(deletesBuckets, times(1)).deleteBucket(bucket);
	}

	public void archiveBucket_bucketIsAlreadyArchivedInFormat_doesNotExportBucket() {
		BucketFormat format = bucketFormats.get(0);
		when(archiveBucketTransferer.isArchived(bucket, format)).thenReturn(true);
		bucketArchiver.archiveBucket(bucket);
		verify(exporter, never()).exportBucket(bucket, format);
	}

}
