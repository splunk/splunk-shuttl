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

import static com.splunk.shuttl.testutil.UtilsFile.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketExporter;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.archive.CsvBucketCreator;
import com.splunk.shuttl.archiver.archive.CsvExporter;
import com.splunk.shuttl.archiver.archive.UnknownBucketFormatException;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.UtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketExporterTest {

    BucketExporter bucketExporter;
    CsvExporter csvExporter;
    CsvBucketCreator csvBucketCreator;

    @BeforeMethod(groups = { "fast-unit" })
    public void setUp() {
	csvExporter = mock(CsvExporter.class);
	csvBucketCreator = mock(CsvBucketCreator.class);
	bucketExporter = new BucketExporter(csvExporter, csvBucketCreator);
    }

    @Test(groups = { "fast-unit" })
    public void exportBucketToFormat_whenBucketIsAlreadyInThatFormat_returnTheSameBucket() {
	Bucket bucket = mock(Bucket.class);
	when(bucket.getFormat()).thenReturn(BucketFormat.SPLUNK_BUCKET);
	Bucket exportedToFormat = bucketExporter.exportBucketToFormat(bucket,
		BucketFormat.SPLUNK_BUCKET);
	assertSame(bucket, exportedToFormat);
    }

    @Test(expectedExceptions = { UnknownBucketFormatException.class })
    public void exportBucketToFormat_formatIsUnknown_throwUnknownBucketFormatException() {
	Bucket bucket = mock(Bucket.class);
	bucketExporter.exportBucketToFormat(bucket, BucketFormat.UNKNOWN);
    }

    public void exportBucketToFormat_exportsSplunkBucketWithCsvExporter_createsAndReturnsBucketFromCsvFile() {
	Bucket bucket = UtilsBucket.createTestBucket();
	File csvFile = createTestFile();
	when(csvExporter.exportBucketToCsv(bucket)).thenReturn(csvFile);
	Bucket csvBucket = mock(Bucket.class);
	when(csvBucketCreator.createBucketWithCsvFile(csvFile, bucket))
		.thenReturn(csvBucket);
	Bucket newBucket = bucketExporter.exportBucketToFormat(bucket,
		BucketFormat.CSV);
	assertEquals(csvBucket, newBucket);
    }

    public void exportBucketToFormat_bucketIsUnknownAndExportingToCsv_throwsUnsupportedOperationException() {
	Bucket unknownFormatedBucket = mock(Bucket.class);
	when(unknownFormatedBucket.getFormat())
		.thenReturn(BucketFormat.UNKNOWN);
	try {
	    bucketExporter.exportBucketToFormat(unknownFormatedBucket,
		    BucketFormat.CSV);
	    fail();
	} catch (UnsupportedOperationException e) {
	}
	verifyZeroInteractions(csvExporter);
    }
}
