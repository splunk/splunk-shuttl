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

import static com.splunk.shuttl.archiver.LocalFileSystemConstants.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketExporter;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.FileNotDirectoryException;
import com.splunk.shuttl.testutil.UtilsBucket;
import com.splunk.shuttl.testutil.UtilsEnvironment;

/**
 * Test real classes and splunk's export tool.
 */
@Test(groups = { "integration" })
public class BucketExporterIntegrationTest {

    private BucketExporter bucketExporter;

    @BeforeMethod
    public void setUp() throws FileNotFoundException,
	    FileNotDirectoryException, URISyntaxException {
	bucketExporter = BucketExporter.create();
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
	Bucket realBucket = UtilsBucket.createRealBucket();
	Bucket csvBucket = bucketExporter.exportBucketToFormat(realBucket,
		BucketFormat.CSV);
	assertEquals(realBucket.getName(), csvBucket.getName());
	assertEquals(BucketFormat.CSV, csvBucket.getFormat());
	assertEquals(1, csvBucket.getDirectory().listFiles().length);
	File csvFile = csvBucket.getDirectory().listFiles()[0];
	assertEquals("csv", FilenameUtils.getExtension(csvFile.getName()));
	long csvFileSize = csvFile.length();
	assertTrue(0 < csvFileSize);
    }

}
