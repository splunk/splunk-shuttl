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
import static org.testng.Assert.*;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public abstract class BucketFileCreatorTest {

	private BucketFileCreator bucketFileCreator;
	private Bucket bucket;
	private File dir;
	private File file;

	@BeforeMethod
	public void setUp() {
		dir = createDirectory();
		file = createFileInParent(dir, "testFile." + getExtension());
		bucket = TUtilsBucket.createBucket();
		bucketFileCreator = new BucketFileCreator(getFormat(), getExtension());
	}

	protected abstract BucketFormat getFormat();

	protected abstract String getExtension();

	@AfterMethod
	public void tearDown() {
		FileUtils.deleteQuietly(dir);
	}

	@Test(groups = { "fast-unit" })
	public void _givenCsvFileAndBucket_bucketNamedAsCsvFileWithoutTheCsvExtension() {
		Bucket csvBucket = bucketFileCreator.createBucketWithCsvFile(file, bucket);
		String fileWithoutExtension = FilenameUtils.removeExtension(file.getName());
		assertEquals(csvBucket.getName(), fileWithoutExtension);
	}

	public void _givenCsvFileAndBucket_newBucketObjectKeepsOldBucketSize() {
		Bucket newBucket = bucketFileCreator.createBucketWithCsvFile(file, bucket);
		assertEquals(bucket.getSize(), newBucket.getSize());

	}

	public void _givenCsvFile_createsBucketInParentFileToCsvFile() {
		Bucket csvBucket = bucketFileCreator.createBucketWithCsvFile(file, bucket);
		assertEquals(dir, csvBucket.getDirectory().getParentFile());
	}

	public void _givenBucket_bucketWithSameIndexAsGivenBucket() {
		Bucket csvBucket = bucketFileCreator.createBucketWithCsvFile(file, bucket);
		assertEquals(bucket.getIndex(), csvBucket.getIndex());
	}

	public void _givenCsvFile_bucketWithCsvFormat() {
		Bucket csvBucket = bucketFileCreator.createBucketWithCsvFile(file, bucket);
		assertEquals(BucketFormat.CSV, csvBucket.getFormat());
	}

	public void _givenCsvFile_csvFileMovedToCsvBucketDirectory() {
		long csvFileSize = file.length();
		Bucket csvBucket = bucketFileCreator.createBucketWithCsvFile(file, bucket);
		File bucketDir = csvBucket.getDirectory();
		assertEquals(1, bucketDir.listFiles().length);
		File movedCsvFile = bucketDir.listFiles()[0];
		assertEquals(file.getName(), movedCsvFile.getName());
		assertEquals(csvFileSize, movedCsvFile.length());
		assertFalse(file.exists());
	}

	public void _givenCsvFile_bucketDirectoryHasSameNameAsCsvFileWithoutExtension() {
		Bucket csvBucket = bucketFileCreator.createBucketWithCsvFile(file, bucket);
		File bucketDir = csvBucket.getDirectory();
		String fileWithoutExtension = FilenameUtils.removeExtension(file.getName());
		assertEquals(fileWithoutExtension, bucketDir.getName());
	}

	// Sad path

	@Test(expectedExceptions = { IllegalArgumentException.class })
	public void _givenNonCsvFile_throwIllegalArgumentException() {
		File notCsvFile = createTestFileWithName("not-csv-file.xkcd");
		assertNotEquals("csv", FilenameUtils.getExtension(notCsvFile.getName()));
		bucketFileCreator.createBucketWithCsvFile(notCsvFile, null);
	}

	@Test(expectedExceptions = { BucketFileNotFoundException.class })
	public void _givenNonExistantCsvFile_throwFileNotFoundException() {
		File nonExistantCsvFile = new File("non-existant-file.csv");
		assertFalse(nonExistantCsvFile.exists());
		assertEquals("csv",
				FilenameUtils.getExtension(nonExistantCsvFile.getName()));
		bucketFileCreator.createBucketWithCsvFile(nonExistantCsvFile, bucket);
	}
}
