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

package com.splunk.shuttl.archiver.importexport;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.archive.UnknownBucketFormatException;
import com.splunk.shuttl.archiver.importexport.BucketExportController.UnknownFormatChangerToFormatException;
import com.splunk.shuttl.archiver.importexport.csv.CsvExporter;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketExportControllerTest {

	BucketExportController bucketExportController;
	CsvExporter csvChanger;

	@BeforeMethod(groups = { "fast-unit" })
	public void setUp() {
		csvChanger = mock(CsvExporter.class);
		Map<BucketFormat, BucketExporter> formatChangers = new HashMap<BucketFormat, BucketExporter>();
		formatChangers.put(BucketFormat.CSV, csvChanger);
		bucketExportController = new BucketExportController(formatChangers);
	}

	@Test(groups = { "fast-unit" })
	public void _whenBucketIsAlreadyInThatFormat_returnTheSameBucket() {
		LocalBucket bucket = mock(LocalBucket.class);
		when(bucket.getFormat()).thenReturn(BucketFormat.SPLUNK_BUCKET);
		Bucket exportedToFormat = bucketExportController.exportBucket(bucket,
				BucketFormat.SPLUNK_BUCKET);
		assertSame(bucket, exportedToFormat);
	}

	@Test(expectedExceptions = { UnknownBucketFormatException.class })
	public void _formatIsUnknown_throwUnknownBucketFormatException() {
		LocalBucket bucket = mock(LocalBucket.class);
		bucketExportController.exportBucket(bucket, BucketFormat.UNKNOWN);
	}

	public void _bucketIsUnknown_throwsUnsupportedOperationException() {
		LocalBucket unknownFormatedBucket = mock(LocalBucket.class);
		when(unknownFormatedBucket.getFormat()).thenReturn(BucketFormat.UNKNOWN);
		try {
			bucketExportController.exportBucket(unknownFormatedBucket, BucketFormat.CSV);
			fail();
		} catch (UnsupportedOperationException e) {
		}
		verifyZeroInteractions(csvChanger);
	}

	public void _givenCsvBucket_exportsSplunkBucketWithCsvExporter() {
		LocalBucket bucket = TUtilsBucket.createBucket();
		LocalBucket csvBucket = mock(LocalBucket.class);
		when(csvChanger.exportBucket(bucket)).thenReturn(csvBucket);
		Bucket newBucket = bucketExportController.exportBucket(bucket, BucketFormat.CSV);
		assertEquals(csvBucket, newBucket);
	}

	@Test(expectedExceptions = { UnknownFormatChangerToFormatException.class })
	public void _givenNonExistingBucketFormatChanger_throws() {
		BucketExportController exporter = new BucketExportController(
				new HashMap<BucketFormat, BucketExporter>());
		exporter.exportBucket(TUtilsBucket.createBucket(), BucketFormat.CSV);
	}
}
