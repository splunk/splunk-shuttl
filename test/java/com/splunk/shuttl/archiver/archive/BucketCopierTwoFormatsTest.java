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
import static org.testng.Assert.*;

import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.importexport.BucketExportController;
import com.splunk.shuttl.archiver.model.LocalBucket;

@Test(groups = { "fast-unit" })
public class BucketCopierTwoFormatsTest {

	private BucketCopier bucketCopier;
	private BucketExportController exporter;
	private ArchiveBucketTransferer archiveBucketTransferer;
	private BucketDeleter bucketDeleter;

	private LocalBucket bucket;
	private List<BucketFormat> formats;
	private LocalBucket exportedBucket1;
	private LocalBucket exportedBucket2;

	@BeforeMethod
	public void setUp() {
		exporter = mock(BucketExportController.class);
		archiveBucketTransferer = mock(ArchiveBucketTransferer.class);
		bucketDeleter = mock(BucketDeleter.class);
		formats = asList(BucketFormat.SPLUNK_BUCKET, BucketFormat.CSV);
		bucketCopier = new BucketCopier(exporter, archiveBucketTransferer, formats,
				bucketDeleter);

		bucket = mock(LocalBucket.class);
	}

	@Test(groups = { "fast-unit" })
	public void copyBucket_givenTwoFormats_archivesBothExportsAndDeletesTheExportedBuckets() {
		setUpTwoFormatsAndTwoExportedBuckets();
		bucketCopier.copyBucket(bucket);

		verify(archiveBucketTransferer).transferBucketToArchive(exportedBucket1);
		verify(archiveBucketTransferer).transferBucketToArchive(exportedBucket2);
	}

	private void setUpTwoFormatsAndTwoExportedBuckets() {
		exportedBucket1 = mock(LocalBucket.class);
		exportedBucket2 = mock(LocalBucket.class);
		when(exporter.exportBucket(bucket, formats.get(0))).thenReturn(
				exportedBucket1);
		when(exporter.exportBucket(bucket, formats.get(1))).thenReturn(
				exportedBucket2);
	}

	public void copyBucket_firstFormatFailsToArchive_stillArchivesTheSecondOne() {
		setUpTwoFormatsAndTwoExportedBuckets();
		throwExceptionWhenTransferingBucket(exportedBucket1);

		try {
			bucketCopier.copyBucket(bucket);
		} catch (RuntimeException e) {
			// Do nothing.
		}

		verify(archiveBucketTransferer).transferBucketToArchive(exportedBucket2);
	}

	private void throwExceptionWhenTransferingBucket(LocalBucket exportedBucket) {
		doThrow(new FailedToArchiveBucketException()).when(archiveBucketTransferer)
				.transferBucketToArchive(exportedBucket);
	}

	public void copyBucket_firstFormatFailsSecondSucceeds_stillDeletsBothExportedBuckets() {
		setUpTwoFormatsAndTwoExportedBuckets();
		throwExceptionWhenTransferingBucket(exportedBucket1);

		try {
			bucketCopier.copyBucket(bucket);
		} catch (RuntimeException e) {
			// Do nothing.
		}
		verify(bucketDeleter).deleteBucket(exportedBucket1);
		verify(bucketDeleter).deleteBucket(exportedBucket2);
	}

	public void copyBucket_firstFormatIsTheSameAsOriginalBucket_dontDeleteOriginalBucket() {
		when(exporter.exportBucket(bucket, formats.get(0))).thenReturn(bucket);
		when(exporter.exportBucket(bucket, formats.get(1))).thenReturn(
				exportedBucket2);

		bucketCopier.copyBucket(bucket);

		verify(bucketDeleter).deleteBucket(exportedBucket2);
		verify(bucketDeleter, never()).deleteBucket(bucket);
	}

	public void copyBucket_bothCopiesFail_deletesBothExportsAndThrows() {
		setUpTwoFormatsAndTwoExportedBuckets();
		throwExceptionWhenTransferingBucket(exportedBucket1);
		throwExceptionWhenTransferingBucket(exportedBucket2);

		try {
			bucketCopier.copyBucket(bucket);
			fail("should have gotten exception");
		} catch (RuntimeException e) {
		}

		verify(bucketDeleter).deleteBucket(exportedBucket1);
		verify(bucketDeleter).deleteBucket(exportedBucket2);
	}
}
