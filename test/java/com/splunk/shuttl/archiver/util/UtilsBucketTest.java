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
package com.splunk.shuttl.archiver.util;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.importexport.NoFileFoundException;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class UtilsBucketTest {

	public void getCsvFile_givenCsvBucket_getsTheCsvFile() {
		LocalBucket csvBucket = TUtilsBucket.createRealCsvBucket();
		File csvFile = UtilsBucket.getCsvFile(csvBucket);
		assertFileIsCsv(csvFile);
	}

	private void assertFileIsCsv(File csvFile) {
		assertEquals("csv", FilenameUtils.getExtension(csvFile.getName()));
	}

	public void getCsvFile_givenCsvBucketWithMoreThanOneFile_getsTheCsvFile() {
		LocalBucket csvBucket = TUtilsBucket.createRealCsvBucket();
		createFileInParent(csvBucket.getDirectory(), "abc.foo");
		createFileInParent(csvBucket.getDirectory(), "xyz.bar");
		assertFileIsCsv(UtilsBucket.getCsvFile(csvBucket));
	}

	@Test(expectedExceptions = { IllegalArgumentException.class })
	public void getCsvFile_emptyBucket_throwsException() {
		LocalBucket emptyBucket = TUtilsBucket.createBucket();
		FileUtils.deleteQuietly(emptyBucket.getDirectory());
		assertTrue(emptyBucket.getDirectory().mkdirs());
		UtilsBucket.getCsvFile(emptyBucket);
	}

	@Test(expectedExceptions = { NoFileFoundException.class })
	public void getCsvFile_noCsvFile_throwsRuntimeException() {
		LocalBucket bucketWithoutCsvFile = TUtilsBucket.createBucket();
		UtilsBucket.getCsvFile(bucketWithoutCsvFile);
	}
}
