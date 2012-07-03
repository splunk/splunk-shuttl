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
package com.splunk.shuttl.archiver.archive;

import static java.util.Arrays.*;
import static org.mockito.Mockito.*;

import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.importexport.BucketExporter;
import com.splunk.shuttl.archiver.model.Bucket;

@Test(groups = { "fast-unit" })
public class BucketArchiverTwoFormatsTest {

	private BucketArchiver bucketArchiver;
	private BucketExporter exporter;
	private ArchiveBucketTransferer archiveBucketTransferer;
	private BucketDeleter bucketDeleter;

	private Bucket bucket;
	private List<BucketFormat> formats;
	private Bucket exportedBucket1;
	private Bucket exportedBucket2;

	@BeforeMethod
	public void setUp() {
		exporter = mock(BucketExporter.class);
		archiveBucketTransferer = mock(ArchiveBucketTransferer.class);
		bucketDeleter = mock(BucketDeleter.class);
		formats = asList(BucketFormat.SPLUNK_BUCKET, BucketFormat.CSV);
		bucketArchiver = new BucketArchiver(exporter, archiveBucketTransferer,
				bucketDeleter, formats);

		bucket = mock(Bucket.class);
	}

	@Test(groups = { "fast-unit" })
	public void archiveBucket_givenTwoFormats_archivesBothExportsAndDeletesOriginalBucket() {
		setUpTwoFormatsAndTwoExportedBuckets();
		bucketArchiver.archiveBucket(bucket);

		verify(archiveBucketTransferer).transferBucketToArchive(exportedBucket1);
		verify(archiveBucketTransferer).transferBucketToArchive(exportedBucket2);
		verify(bucketDeleter).deleteBucket(bucket);
	}

	private void setUpTwoFormatsAndTwoExportedBuckets() {
		exportedBucket1 = mock(Bucket.class);
		exportedBucket2 = mock(Bucket.class);
		when(exporter.exportBucket(bucket, formats.get(0))).thenReturn(
				exportedBucket1);
		when(exporter.exportBucket(bucket, formats.get(1))).thenReturn(
				exportedBucket2);
	}

	public void archiveBucket_firstFormatFailsToArchive_stillArchivesTheSecondOne() {
		setUpTwoFormatsAndTwoExportedBuckets();
		doThrow(new FailedToArchiveBucketException()).when(archiveBucketTransferer)
				.transferBucketToArchive(exportedBucket1);

		bucketArchiver.archiveBucket(bucket);
		verify(archiveBucketTransferer).transferBucketToArchive(exportedBucket2);
	}

	public void archiveBucket_firstFormatFailsSecondSucceeds_deletesBothExportsButNotOriginalBucket() {
		setUpTwoFormatsAndTwoExportedBuckets();
		doThrow(new FailedToArchiveBucketException()).when(archiveBucketTransferer)
				.transferBucketToArchive(exportedBucket1);

		bucketArchiver.archiveBucket(bucket);
		verify(bucketDeleter).deleteBucket(exportedBucket1);
		verify(bucketDeleter).deleteBucket(exportedBucket2);
		verify(bucketDeleter, never()).deleteBucket(bucket);
	}

	public void archiveBucket_firstFormatIsTheSameAsOriginalBucket_dontDeleteFirstBucketAfterArchiving() {
		when(exporter.exportBucket(bucket, formats.get(0))).thenReturn(bucket);
		when(exporter.exportBucket(bucket, formats.get(1))).thenReturn(
				exportedBucket2);

		bucketArchiver.archiveBucket(bucket);

		verify(bucketDeleter).deleteBucket(exportedBucket2);
		verify(bucketDeleter, times(1)).deleteBucket(bucket);
	}

}
