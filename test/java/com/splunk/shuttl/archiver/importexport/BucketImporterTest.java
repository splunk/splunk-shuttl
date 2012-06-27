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
package com.splunk.shuttl.archiver.importexport;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.csv.CsvImporter;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketImporterTest {

	private BucketImporter bucketImporter;
	private CsvImporter csvImporter;

	@BeforeMethod
	public void setUp() {
		csvImporter = mock(CsvImporter.class);
		bucketImporter = new BucketImporter(csvImporter);
	}

	@Test(groups = { "fast-unit" })
	public void _bucketInSplunkBucketFormat_sameBucket() {
		Bucket bucket = TUtilsBucket.createBucket();
		assertEquals(BucketFormat.SPLUNK_BUCKET, bucket.getFormat());
		Bucket restoredBucket = bucketImporter.restoreToSplunkBucketFormat(bucket);
		assertTrue(restoredBucket == bucket);
	}

	public void _bucketInCsvFormat_returnBucketFromCsvImporter() {
		Bucket realCsvBucket = TUtilsBucket.createRealCsvBucket();
		Bucket importedBucket = mock(Bucket.class);
		when(csvImporter.importBucketFromCsv(realCsvBucket)).thenReturn(
				importedBucket);
		Bucket restoredBucket = bucketImporter
				.restoreToSplunkBucketFormat(realCsvBucket);
		assertEquals(importedBucket, restoredBucket);
	}

	@Test(expectedExceptions = { UnsupportedOperationException.class })
	public void _bucketInUnknownFormat_throwsUnsupportedOperationException() {
		Bucket unknownBucket = mock(Bucket.class);
		when(unknownBucket.getFormat()).thenReturn(BucketFormat.UNKNOWN);
		bucketImporter.restoreToSplunkBucketFormat(unknownBucket);
	}

}
