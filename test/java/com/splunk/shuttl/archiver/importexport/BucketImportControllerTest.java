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

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.csv.CsvImporter;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketImportControllerTest {

	private BucketImportController bucketImportController;
	private CsvImporter csvImporter;

	@BeforeMethod
	public void setUp() {
		csvImporter = mock(CsvImporter.class);
		Map<BucketFormat, BucketImporter> importers = new HashMap<BucketFormat, BucketImporter>();
		importers.put(BucketFormat.CSV, csvImporter);
		bucketImportController = new BucketImportController(importers);
	}

	@Test(groups = { "fast-unit" })
	public void _bucketInSplunkBucketFormat_sameBucket() {
		LocalBucket bucket = TUtilsBucket.createBucket();
		assertEquals(BucketFormat.SPLUNK_BUCKET, bucket.getFormat());
		Bucket restoredBucket = bucketImportController
				.restoreToSplunkBucketFormat(bucket);
		assertTrue(restoredBucket == bucket);
	}

	public void _bucketInCsvFormat_returnBucketFromCsvImporter() {
		LocalBucket realCsvBucket = TUtilsBucket.createRealCsvBucket();
		LocalBucket importedBucket = mock(LocalBucket.class);
		when(csvImporter.importBucket(realCsvBucket)).thenReturn(importedBucket);
		Bucket restoredBucket = bucketImportController
				.restoreToSplunkBucketFormat(realCsvBucket);
		assertEquals(importedBucket, restoredBucket);
	}

	@Test(expectedExceptions = { UnsupportedOperationException.class })
	public void _bucketInUnknownFormat_throwsUnsupportedOperationException() {
		LocalBucket unknownBucket = mock(LocalBucket.class);
		when(unknownBucket.getFormat()).thenReturn(BucketFormat.UNKNOWN);
		bucketImportController.restoreToSplunkBucketFormat(unknownBucket);
	}

}
