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

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.csv.BucketToCsvFileExporter;
import com.splunk.shuttl.archiver.importexport.csv.CsvBzip2Exporter;
import com.splunk.shuttl.archiver.importexport.csv.CsvExporter;
import com.splunk.shuttl.archiver.importexport.csv.CsvGzipExporter;
import com.splunk.shuttl.archiver.importexport.csv.CsvSnappyExporter;
import com.splunk.shuttl.archiver.importexport.tgz.CreatesBucketTgz;
import com.splunk.shuttl.archiver.importexport.tgz.TgzFormatExporter;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsEnvironment;

/**
 * Test real classes and splunk's export tool.
 */
@Test(groups = { "end-to-end" })
public class BucketExportControllerIntegrationTest {

	private BucketExportController bucketExportController;
	private LocalFileSystemPaths localFileSystemPaths;

	@BeforeMethod
	public void setUp() {
		localFileSystemPaths = new LocalFileSystemPaths(createDirectory());
		CsvExporter csvExporter = CsvExporter.create(BucketToCsvFileExporter
				.create(localFileSystemPaths));
		bucketExportController = BucketExportController
				.create(csvExporter, TgzFormatExporter.create(CreatesBucketTgz
						.create(localFileSystemPaths)), CsvSnappyExporter.create(
						csvExporter, localFileSystemPaths), CsvBzip2Exporter.create(
						csvExporter, localFileSystemPaths), CsvGzipExporter.create(
						csvExporter, localFileSystemPaths));
	}

	@AfterMethod
	public void tearDown() {
		FileUtils.deleteQuietly(localFileSystemPaths.getArchiverDirectory());
	}

	@Test(groups = { "end-to-end" })
	@Parameters(value = { "splunk.home" })
	public void exportBucketToFormat_splunkHomeSetExportingBucketWithRealDataToCsv_createsCsvBucket(
			final String splunkHome) {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
				exportingBucketWithRealDataToCsvCreatesCsvBucket();
			}
		});
	}

	private void exportingBucketWithRealDataToCsvCreatesCsvBucket() {
		LocalBucket realBucket = TUtilsBucket.createRealBucket();
		LocalBucket csvBucket = bucketExportController.exportBucket(realBucket,
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
