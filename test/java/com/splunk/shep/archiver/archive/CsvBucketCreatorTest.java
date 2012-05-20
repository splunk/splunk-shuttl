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

import static com.splunk.shep.testutil.UtilsFile.*;
import static org.apache.commons.io.FilenameUtils.*;
import static org.testng.Assert.*;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;

@Test(groups = { "fast-unit" })
public class CsvBucketCreatorTest {

    private CsvBucketCreator csvBucketCreator;
    private Bucket bucket;
    private File dir;
    private File csvFile;

    @BeforeMethod
    public void setUp() {
	dir = createTempDirectory();
	csvFile = createFileInParent(dir, "csvFile.csv");
	bucket = UtilsBucket.createTestBucket();
	csvBucketCreator = new CsvBucketCreator();
    }

    @AfterMethod
    public void tearDown() {
	FileUtils.deleteQuietly(dir);
    }

    @Test(groups = { "fast-unit" })
    public void _givenCsvFileAndBucket_bucketNamedAsCsvFileWithoutTheCsvExtension() {
	Bucket csvBucket = csvBucketCreator.createBucketWithCsvFile(csvFile,
		bucket);
	String fileWithoutExtension = FilenameUtils.removeExtension(csvFile
		.getName());
	assertEquals(csvBucket.getName(), fileWithoutExtension);
    }

    public void _givenCsvFile_createsBucketInParentFileToCsvFile() {
	Bucket csvBucket = csvBucketCreator.createBucketWithCsvFile(csvFile,
		bucket);
	assertEquals(dir, csvBucket.getDirectory().getParentFile());
    }

    public void _givenBucket_bucketWithSameIndexAsGivenBucket() {
	Bucket csvBucket = csvBucketCreator.createBucketWithCsvFile(csvFile,
		bucket);
	assertEquals(bucket.getIndex(), csvBucket.getIndex());
    }

    public void _givenCsvFile_bucketWithCsvFormat() {
	Bucket csvBucket = csvBucketCreator.createBucketWithCsvFile(csvFile,
		bucket);
	assertEquals(BucketFormat.CSV, csvBucket.getFormat());
    }

    public void _givenCsvFile_csvFileMovedToCsvBucketDirectory() {
	long csvFileSize = csvFile.length();
	Bucket csvBucket = csvBucketCreator.createBucketWithCsvFile(csvFile,
		bucket);
	File bucketDir = csvBucket.getDirectory();
	assertEquals(1, bucketDir.listFiles().length);
	File movedCsvFile = bucketDir.listFiles()[0];
	assertEquals(csvFile.getName(), movedCsvFile.getName());
	assertEquals(csvFileSize, movedCsvFile.length());
	assertFalse(csvFile.exists());
    }

    public void _givenCsvFile_bucketDirectoryHasSameNameAsCsvFileWithoutExtension() {
	Bucket csvBucket = csvBucketCreator.createBucketWithCsvFile(csvFile,
		bucket);
	File bucketDir = csvBucket.getDirectory();
	String fileWithoutExtension = FilenameUtils.removeExtension(csvFile
		.getName());
	assertEquals(fileWithoutExtension, bucketDir.getName());
    }

    // Sad path

    @Test(expectedExceptions = { IllegalArgumentException.class })
    public void _givenNonCsvFile_throwIllegalArgumentException() {
	File notCsvFile = createTestFileWithName("not-csv-file.xkcd");
	assertNotEquals("csv", getExtension(notCsvFile.getName()));
	csvBucketCreator.createBucketWithCsvFile(notCsvFile, null);
    }

    @Test(expectedExceptions = { CsvFileNotFoundException.class })
    public void _givenNonExistantCsvFile_throwFileNotFoundException() {
	File nonExistantCsvFile = new File("non-existant-file.csv");
	assertFalse(nonExistantCsvFile.exists());
	assertEquals("csv", getExtension(nonExistantCsvFile.getName()));
	csvBucketCreator.createBucketWithCsvFile(nonExistantCsvFile, bucket);
    }
}
