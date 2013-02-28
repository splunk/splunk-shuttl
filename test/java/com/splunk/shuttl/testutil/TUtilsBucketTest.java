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
import com.splunk.shuttl.archiver.model.LocalBucket;

@Test(groups = { "fast-unit" })
public class TUtilsBucketTest {

	LocalBucket bucket;

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
			new LocalBucket(bucketDir, "index-name", BucketFormat.SPLUNK_BUCKET);
		} catch (Exception e) {
			TUtilsTestNG.failForException("Coudn't create a valid bucket dir", e);
		}
	}

	public void createBucketInDirectory_givenDirectory_createsBucketInThatDirectory() {
		File parent = TUtilsFile.createDirectory();
		LocalBucket bucketCreated = TUtilsBucket.createBucketInDirectory(parent);
		assertEquals(parent, bucketCreated.getDirectory().getParentFile());
	}

	public void createBucketInDirectoryWithTimes_givenDirectory_createsBucketInTheDirectory() {
		File parent = null;
		try {
			parent = createDirectory();
			LocalBucket bucket = TUtilsBucket.createBucketInDirectoryWithTimes(
					parent, new Date(), new Date());
			assertEquals(parent.getAbsolutePath(), bucket.getDirectory()
					.getParentFile().getAbsolutePath());
		} finally {
			FileUtils.deleteQuietly(parent);
		}
	}

	public void createBucketInDirectoryWithTimes_givenTimesInMilliseconds_bucketNameStartsWith_db_earliest_latest_InSeconds() {
		File parent = null;
		try {
			parent = createDirectory();
			Date earliest = TUtilsDate.getNowWithoutMillis();
			Date latest = TUtilsDate.getLaterDate(earliest);
			Bucket bucketWithTimes = TUtilsBucket.createBucketInDirectoryWithTimes(
					parent, earliest, latest);

			long latestInSeconds = latest.getTime() / 1000;
			long earliestInSeconds = earliest.getTime() / 1000;
			String expectedBucketNameStart = "db_" + latestInSeconds + "_"
					+ earliestInSeconds;
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
		Date earliest = TUtilsDate.getNowWithoutMillis();
		Date latest = TUtilsDate.getLaterDate(earliest);
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

	public void createInDirectoryWithTimesAndIndex_givenIndex_hasSpecifiedIndex() {
		Bucket b = TUtilsBucket.createBucketInDirectoryWithTimesAndIndex(
				getShuttlTestDirectory(), new Date(), new Date(), "foo");
		assertEquals("foo", b.getIndex());
	}

	public void createBucketWithIndex_givenIndex_hasIndex() {
		LocalBucket b = TUtilsBucket.createBucketWithIndex("index");
		assertEquals(b.getIndex(), "index");
	}

	@Test(groups = { "slow-unit" })
	public void createRealSplunkBucketTgz_createSuccess_bucketHasSplunkBucketTgzFormat() {
		assertEquals(BucketFormat.SPLUNK_BUCKET_TGZ, TUtilsBucket
				.createRealSplunkBucketTgz().getFormat());
	}

	public void createTgzBucket_noArgs_hasSplunkBucketTgzFormat() {
		assertEquals(BucketFormat.SPLUNK_BUCKET_TGZ, TUtilsBucket.createTgzBucket()
				.getFormat());
	}

	public void createTgzBucket_noArgs_hasOneTgzFileInBucket() {
		File[] bucketFiles = TUtilsBucket.createTgzBucket().getDirectory()
				.listFiles();
		assertEquals(1, bucketFiles.length);
		assertTrue(bucketFiles[0].getName().endsWith("tgz"));
	}

	public void createReplicatedBucket__nameStartsWith_rb() {
		Bucket bucket = TUtilsBucket.createReplicatedBucket("foo",
				createDirectory(), "baz");
		assertTrue(bucket.getName().startsWith("rb"));
	}

	public void createReplicatedBucket_parent_directoryIsInParent() {
		File parent = createDirectory();
		LocalBucket b = TUtilsBucket.createReplicatedBucket("foo", parent, "baz");
		assertEquals(b.getDirectory().getParentFile().getAbsolutePath(),
				parent.getAbsolutePath());
	}

	public void createReplicatedBucket_guid_endsWith_guid() {
		Bucket b = TUtilsBucket.createReplicatedBucket("foo", createDirectory(),
				"baz");
		assertEquals(b.getGuid(), "baz");
	}

	public void createReplicatedBucket_index_hasThatIndex() {
		String index = "replicated_index";
		LocalBucket b = TUtilsBucket.createReplicatedBucket(index,
				createDirectory(), "baz");
		assertEquals(index, b.getIndex());
	}

	public void replaceEverythingAfterEarliestTimeWithIndexAndGuid_indexAndGuid_endsWithIndexAndGuid() {
		String name = bucket.getName();
		long newIndex = 1234;
		String newName = TUtilsBucket
				.replaceEverythingAfterEarliestTimeWithIndexAndGuid(name, 1234, "guid");
		assertTrue(newName.endsWith(newIndex + "_" + "guid"));

		int idx = newName.lastIndexOf(newIndex + "");
		String removedLastPart = newName.substring(0, idx);
		assertTrue(bucket.getName().startsWith(removedLastPart));
	}

	@Test(groups = { "slow-unit" })
	public void createRealReplicatedBucket_parameters_hasParameterProperties() {
		File parent = createDirectory();
		LocalBucket bucket = TUtilsBucket.createRealReplicatedBucket("theIndex",
				parent, "theGuid");
		assertEquals("theIndex", bucket.getIndex());
		assertEquals(parent.getAbsolutePath(), bucket.getDirectory()
				.getParentFile().getAbsolutePath());
		assertEquals(bucket.getGuid(), "theGuid");
	}

	@Test(groups = { "slow-unit" })
	public void createRealReplicatedBucket__isACopyOfARealBucket() {
		LocalBucket realBucket = TUtilsBucket.createRealBucket();
		LocalBucket replicated = TUtilsBucket.createRealReplicatedBucket("foo",
				createDirectory(), "baz");
		TUtilsTestNG.assertDirectoriesAreCopies(realBucket.getDirectory(),
				replicated.getDirectory());
	}

	@Test(groups = { "slow-unit" })
	public void createRealReplicatedBucket__isReplicatedBucket() {
		LocalBucket rb = TUtilsBucket.createRealReplicatedBucket("foo",
				createDirectory(), "guid");
		assertTrue(rb.isReplicatedBucket());
	}
}
