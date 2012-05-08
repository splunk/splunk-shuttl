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
package com.splunk.shep.archiver.archive;

import static com.splunk.shep.archiver.LocalFileSystemConstants.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.model.FileNotDirectoryException;
import com.splunk.shep.testutil.UtilsEnvironment;

/**
 * Test real classes and splunk's export tool.
 */
@Test(groups = { "integration" })
public class BucketExporterIntegrationTest {

    private static final URL realBucketUrl = BucketExporterIntegrationTest.class
	    .getResource("/splunk-buckets/db_1336330530_1336330530_0");

    private BucketExporter bucketExporter;
    private Bucket bucket;

    @BeforeMethod
    public void setUp() throws FileNotFoundException,
	    FileNotDirectoryException, URISyntaxException {
	bucketExporter = BucketExporter.create();
	File bucketDirectory = new File(realBucketUrl.toURI()).getAbsoluteFile();
	bucket = new Bucket("index", bucketDirectory);
    }

    @AfterMethod
    public void tearDown() {
	FileUtils.deleteQuietly(getArchiverDirectory());
    }

    @Test(groups = { "integration" })
    @Parameters(value = { "splunk.home" })
    public void exportBucketToFormat_splunkHomeSetExportingBucketWithRealDataToCsv_createsCsvBucket(
	    final String splunkHome) {
	UtilsEnvironment.runInCleanEnvironment(new Runnable() {

	    @Override
	    public void run() {
		UtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME",
			splunkHome);
		exportingBucketWithRealDataToCsvCreatesCsvBucket();
	    }
	});
    }

    private void exportingBucketWithRealDataToCsvCreatesCsvBucket() {
	Bucket csvBucket = bucketExporter.exportBucketToFormat(bucket,
		BucketFormat.CSV);
	assertEquals(BucketFormat.CSV, csvBucket.getFormat());
	assertEquals(1, csvBucket.getDirectory().listFiles().length);
	File csvFile = csvBucket.getDirectory().listFiles()[0];
	assertEquals("csv", FilenameUtils.getExtension(csvFile.getName()));
	long csvFileSize = csvFile.length();
	assertTrue(0 < csvFileSize);
    }

}
