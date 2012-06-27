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
package com.splunk.shuttl.archiver.importexport.csv;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class GetsBucketsCsvFileTest {

	private File csvDirectory;
	private GetsBucketsCsvExportFile getsBucketsCsvExportFile;
	private Bucket bucket;

	@BeforeMethod
	public void setUp() {
		csvDirectory = createDirectory();
		getsBucketsCsvExportFile = new GetsBucketsCsvExportFile(csvDirectory);
		bucket = TUtilsBucket.createBucket();
	}

	@AfterMethod
	public void tearDown() {
		FileUtils.deleteQuietly(csvDirectory);
	}

	@Test(groups = { "fast-unit" })
	public void getCsvOuputFileFromBucket_givenCsvDirectory_createsFileInTheDirectory() {
		File csvFile = getsBucketsCsvExportFile.getCsvFile(bucket);
		assertEquals(csvDirectory, csvFile.getParentFile());
	}

	public void getCsvOutputFileFromBucket_givenBucket_containsBucketNameForUniquness() {
		File csvFile = getsBucketsCsvExportFile.getCsvFile(bucket);
		assertTrue(csvFile.getName().contains(bucket.getName()));
	}

	public void getCsvOuputFileFromBucket_givenBucket_hasCsvExtension() {
		File csvFile = getsBucketsCsvExportFile.getCsvFile(bucket);
		assertTrue(csvFile.getName().endsWith(".csv"));
	}

	public void getCsvOuputFileFromBucket_givenBucket_doesNotExist() {
		assertFalse(getsBucketsCsvExportFile.getCsvFile(bucket).exists());
	}

	public void getCsvOutputFileFromBucket_givenFileAlreadyExists_doesNotExist()
			throws IOException {
		File csvFile = getsBucketsCsvExportFile.getCsvFile(bucket);
		assertTrue(csvFile.createNewFile());
		File csvFile2 = getsBucketsCsvExportFile.getCsvFile(bucket);
		assertFalse(csvFile2.exists());
	}

}
