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
package com.splunk.shuttl.archiver.endtoend;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.BucketImporter;
import com.splunk.shuttl.archiver.importexport.NoFileFoundException;
import com.splunk.shuttl.archiver.importexport.csv.BucketToCsvFileExporter;
import com.splunk.shuttl.archiver.importexport.csv.CsvBzip2Exporter;
import com.splunk.shuttl.archiver.importexport.csv.CsvBzip2Importer;
import com.splunk.shuttl.archiver.importexport.csv.CsvExporter;
import com.splunk.shuttl.archiver.importexport.csv.CsvImporter;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.util.UtilsBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsEnvironment;
import com.splunk.shuttl.testutil.TUtilsMBean;
import com.splunk.shuttl.testutil.TUtilsTestNG;

public class Bzip2CsvExAndImportEndToEndTest {

	@Test(groups = { "end-to-end" })
	@Parameters(value = { "shuttl.conf.dir", "splunk.home" })
	public void _givenCsvBucket_shouldBeAbleToExportToBzip2AndImportBack(
			final String shuttlConfDir, final String splunkHome) {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
				TUtilsMBean.runWithRegisteredMBeans(new File(shuttlConfDir),
						new Runnable() {

							@Override
							public void run() {
								try {
									runWithSplunkHome();
								} catch (IOException e) {
									TUtilsTestNG.failForException(null, e);
								}
							}
						});
			}
		});
	}

	private void runWithSplunkHome() throws IOException {
		LocalFileSystemPaths localFileSystemPaths = LocalFileSystemPaths.create();
		try {
			CsvExporter csvExporter = CsvExporter.create(BucketToCsvFileExporter
					.create(localFileSystemPaths));
			CsvBzip2Exporter exporter = CsvBzip2Exporter.create(csvExporter,
					localFileSystemPaths);

			LocalBucket realBucket = TUtilsBucket.createRealBucket();
			LocalBucket realCsvBucket = csvExporter.exportBucket(realBucket);
			String realCsvBucketContent = readCsvContent(realCsvBucket);
			realCsvBucket.deleteBucket();

			LocalBucket bzip2Bucket = exporter.exportBucket(realBucket);
			assertEquals(bzip2Bucket.getFormat(), BucketFormat.CSV_BZIP2);
			assertOriginalCsvFileIsDeleted(bzip2Bucket);
			assertEquals(1, bzip2Bucket.getDirectory().listFiles().length);

			File bzip2File = UtilsBucket.getFileFromBucket(bzip2Bucket,
					BucketFormat.CSV_BZIP2);
			CompressionInputStream bzip2In = new BZip2Codec()
					.createInputStream(IOUtils.toBufferedInputStream(FileUtils
							.openInputStream(bzip2File)));
			try {
				String bzip2Content = IOUtils.toString(bzip2In);
				assertEquals(realCsvBucketContent, bzip2Content);
			} finally {
				IOUtils.closeQuietly(bzip2In);
			}

			BucketImporter importer = CsvBzip2Importer.create(CsvImporter.create());
			LocalBucket importedBucket = importer.importBucket(bzip2Bucket);
			assertEquals(importedBucket.getFormat(), BucketFormat.SPLUNK_BUCKET);
			LocalBucket importedCsvBucket = csvExporter.exportBucket(importedBucket);
			String importedCsvContent = readCsvContent(importedCsvBucket);

			assertEquals(realCsvBucketContent, importedCsvContent);
		} finally {
			FileUtils.deleteDirectory(localFileSystemPaths.getArchiverDirectory());
		}
	}

	private String readCsvContent(LocalBucket realCsvBucket) throws IOException {
		return FileUtils.readFileToString(UtilsBucket.getFileFromBucket(
				realCsvBucket, realCsvBucket.getFormat()));
	}

	private void assertOriginalCsvFileIsDeleted(LocalBucket bzip2Bucket) {
		try {
			UtilsBucket.getFileFromBucket(bzip2Bucket, BucketFormat.CSV);
			Assert.fail();
		} catch (NoFileFoundException e) {
			// expected
		}
	}
}
