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

package com.splunk.shuttl.testutil;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.Bucket;

@Test(groups = { "fast-unit" })
public class TUtilsBucketTest {

	Bucket bucket;

	@BeforeMethod
	public void setUp() {
		bucket = TUtilsBucket.createBucket();
	}

	@Test(groups = { "fast-unit" })
	public void createTestBucket_validArguments_createsExitingBucketDir() {
		assertTrue(bucket.getDirectory().exists());
		assertTrue(bucket.getDirectory().isDirectory());
	}

	public void createTestBucket_validArguments_createsWithIndexName() {
		String indexName = bucket.getIndex();
		assertTrue("Index name was " + indexName, indexName.matches("index-\\d*"));
	}

	public void createTestBucket_validArguments_createsWithRawdataDir() {
		File rawDataDir = new File(bucket.getDirectory(), "rawdata");
		assertTrue(rawDataDir.exists());
		assertTrue(rawDataDir.isDirectory());
	}

	public void createTestBucket_validArguments_createsWithValidBucketName() {
		String name = bucket.getName();
		String[] nameComponents = name.split("_");

		assertEquals(4, nameComponents.length);
		assertEquals("db", nameComponents[0]);
		try {
			long latest = Long.parseLong(nameComponents[1]);
			long earliest = Long.parseLong(nameComponents[2]);
			Long.parseLong(nameComponents[3]);
			assertTrue(earliest < latest);
		} catch (NumberFormatException e) {
			TUtilsTestNG.failForException("Couldn't parse the numbers", e);
		}
	}

	public void createTestBucket_validArguments_createsWithSplunkFormat() {
		assertEquals(BucketFormat.SPLUNK_BUCKET, bucket.getFormat());
	}

	public void createTestBucketWithIndexAndName_validArguments_correctNameAndIndex() {
		String bucketName = "db_12351290_12351235_1";
		Bucket bucket = TUtilsBucket.createBucketWithIndexAndName("index-name",
				bucketName);
		assertEquals("index-name", bucket.getIndex());
		assertEquals(bucketName, bucket.getName());

	}

	public void createFileFormatedAsBucket_validArgument_theDirCanBeUsedToCreateABucket() {
		File bucketDir = TUtilsBucket
				.createFileFormatedAsBucket("db_12351290_12351235_1");
		try {
			new Bucket("index-name", bucketDir, BucketFormat.SPLUNK_BUCKET);
		} catch (Exception e) {
			TUtilsTestNG.failForException("Coudn't create a valid bucket dir", e);
		}
	}

	public void createBucketInDirectory_givenDirectory_createsBucketInThatDirectory() {
		File parent = TUtilsFile.createDirectory();
		Bucket bucketCreated = TUtilsBucket.createBucketInDirectory(parent);
		assertEquals(parent, bucketCreated.getDirectory().getParentFile());
	}

	public void createBucketWithTimes_givenEarliestLatest_bucketNameStartsWith_db_earliest_latest() {
		Date earliest = new Date(12351235);
		Date latest = new Date(earliest.getTime() + 100);
		Bucket bucketWithTimes = TUtilsBucket.createBucketWithTimes(earliest,
				latest);
		String expectedBucketNameStart = "db_" + latest.getTime() + "_"
				+ earliest.getTime();
		assertTrue(bucketWithTimes.getName().startsWith(expectedBucketNameStart));
	}

	public void createBucketInDirectoryWithTimes_givenDirectory_createsBucketInTheDirectory() {
		File parent = null;
		try {
			parent = createDirectory();
			Bucket bucket = TUtilsBucket.createBucketInDirectoryWithTimes(parent,
					new Date(), new Date());
			assertEquals(parent.getAbsolutePath(), bucket.getDirectory()
					.getParentFile().getAbsolutePath());
		} finally {
			FileUtils.deleteQuietly(parent);
		}
	}

	public void createBucketInDirectoryWithTimes_givenTimes_bucketNameStartsWith_db_earliest_latest() {
		File parent = null;
		try {
			parent = createDirectory();
			Date earliest = new Date();
			Date latest = new Date(earliest.getTime() + 100);
			Bucket bucketWithTimes = TUtilsBucket.createBucketInDirectoryWithTimes(
					parent, earliest, latest);
			String expectedBucketNameStart = "db_" + latest.getTime() + "_"
					+ earliest.getTime();
			assertTrue(bucketWithTimes.getName().startsWith(expectedBucketNameStart));
		} finally {
			FileUtils.deleteQuietly(parent);
		}
	}

	public void createBucketWithName_givenName_bucketWithName() {
		String name = "name";
		Bucket bucket = TUtilsBucket.createBucketWithName(name);
		assertEquals(name, bucket.getName());
	}

	public void createBucketWithIndexAndTimeRange_givenParameters_bucketWithParameters() {
		String index = "index";
		Date earliest = new Date(12345678);
		Date latest = new Date(earliest.getTime() + 100);
		Bucket bucket = TUtilsBucket.createBucketWithIndexAndTimeRange(index,
				earliest, latest);
		assertEquals(index, bucket.getIndex());
		assertEquals(earliest, bucket.getEarliest());
		assertEquals(latest, bucket.getLatest());
	}

	@Test(groups = { "slow-unit" })
	public void createRealBucket_givenRealBucketExistsInTestResources_copyOfTheRealBucket()
			throws URISyntaxException {
		File realBucket = new File(TUtilsBucket.REAL_BUCKET_URL.toURI());
		assertTrue(realBucket.exists());
		File copyBucket = TUtilsBucket.createRealBucket().getDirectory();
		TUtilsTestNG.assertDirectoriesAreCopies(realBucket, copyBucket);
	}

	@Test(groups = { "slow-unit" })
	public void createRealBucket_createSuccess_createdBucketHasSameNameAsRealBucket()
			throws URISyntaxException {
		File realBucket = new File(TUtilsBucket.REAL_BUCKET_URL.toURI());
		File copyBucket = TUtilsBucket.createRealBucket().getDirectory();
		assertEquals(realBucket.getName(), copyBucket.getName());
	}

	@Test(groups = { "slow-unit" })
	public void createRealCsvBucket_givenRealCsvBucketExists_copyOfRealCsvBucket()
			throws URISyntaxException {
		File realCsvBucket = new File(TUtilsBucket.REAL_CSV_BUCKET_URL.toURI());
		assertTrue(realCsvBucket.exists());
		File copyOfCsvBucket = TUtilsBucket.createRealCsvBucket().getDirectory();
		TUtilsTestNG.assertDirectoriesAreCopies(realCsvBucket, copyOfCsvBucket);
	}

	@Test(groups = { "slow-unit" })
	public void createRealCsvBucket_createSuccess_createdBucketHasSameNameAsRealCsvBucket()
			throws URISyntaxException {
		File realCsvBucket = new File(TUtilsBucket.REAL_CSV_BUCKET_URL.toURI());
		File createdCsvBucket = TUtilsBucket.createRealCsvBucket().getDirectory();
		assertEquals(realCsvBucket.getName(), createdCsvBucket.getName());
	}

	@Test(groups = { "slow-unit" })
	public void createRealCsvBucket_createSuccess_bucketHasCsvFormat() {
		Bucket csvBucket = TUtilsBucket.createRealCsvBucket();
		assertEquals(BucketFormat.CSV, csvBucket.getFormat());
	}

	public void createRemoteBucket_noArguments_isRemote() {
		assertTrue(TUtilsBucket.createRemoteBucket().isRemote());
	}

	public void createRemoteBucket_noArguments_hasIndex() {
		assertTrue(TUtilsBucket.createRemoteBucket().getIndex() != null);
	}

	public void createRemoteBucket_noArguments_hasNameWithDates() {
		assertTrue(TUtilsBucket.createRemoteBucket().getName() != null);
		assertTrue(TUtilsBucket.createRemoteBucket().getEarliest() != null);
		assertTrue(TUtilsBucket.createRemoteBucket().getLatest() != null);
	}

	public void createRemoteBucket_noArguments_hasSplunkBucketFormat() {
		Bucket remoteBucket = TUtilsBucket.createRemoteBucket();
		assertEquals(BucketFormat.SPLUNK_BUCKET, remoteBucket.getFormat());
	}

	public void createInDirectory_someDir_earliestAndLatestAreEarlierAndLaterThanEachother() {
		Bucket bucket = TUtilsBucket.createBucketInDirectory(createDirectory());
		assertFalse(bucket.getEarliest().after(bucket.getLatest()));
		assertFalse(bucket.getLatest().before(bucket.getEarliest()));
	}
}
