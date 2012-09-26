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
import com.splunk.shuttl.archiver.importexport.csv.CsvExporter;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketExporterTest {

	BucketExporter bucketExporter;
	CsvExporter csvChanger;

	@BeforeMethod(groups = { "fast-unit" })
	public void setUp() {
		csvChanger = mock(CsvExporter.class);
		Map<BucketFormat, BucketFormatChanger> formatChangers = new HashMap<BucketFormat, BucketFormatChanger>();
		formatChangers.put(BucketFormat.CSV, csvChanger);
		bucketExporter = new BucketExporter(formatChangers);
	}

	@Test(groups = { "fast-unit" })
	public void _whenBucketIsAlreadyInThatFormat_returnTheSameBucket() {
		Bucket bucket = mock(Bucket.class);
		when(bucket.getFormat()).thenReturn(BucketFormat.SPLUNK_BUCKET);
		Bucket exportedToFormat = bucketExporter.exportBucket(bucket,
				BucketFormat.SPLUNK_BUCKET);
		assertSame(bucket, exportedToFormat);
	}

	@Test(expectedExceptions = { UnknownBucketFormatException.class })
	public void _formatIsUnknown_throwUnknownBucketFormatException() {
		Bucket bucket = mock(Bucket.class);
		bucketExporter.exportBucket(bucket, BucketFormat.UNKNOWN);
	}

	public void _bucketIsUnknown_throwsUnsupportedOperationException() {
		Bucket unknownFormatedBucket = mock(Bucket.class);
		when(unknownFormatedBucket.getFormat()).thenReturn(BucketFormat.UNKNOWN);
		try {
			bucketExporter.exportBucket(unknownFormatedBucket, BucketFormat.CSV);
			fail();
		} catch (UnsupportedOperationException e) {
		}
		verifyZeroInteractions(csvChanger);
	}

	public void _givenCsvBucket_exportsSplunkBucketWithCsvExporter() {
		Bucket bucket = TUtilsBucket.createBucket();
		Bucket csvBucket = mock(Bucket.class);
		when(csvChanger.changeFormat(bucket)).thenReturn(csvBucket);
		Bucket newBucket = bucketExporter.exportBucket(bucket, BucketFormat.CSV);
		assertEquals(csvBucket, newBucket);
	}

}
