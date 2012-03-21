package com.splunk.shep.testutil;

import static com.splunk.shep.testutil.UtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.BucketFormat;
import com.splunk.shep.archiver.model.Bucket;

@Test(groups = { "fast" })
public class UtilsBucketTest {

    Bucket bucket;

    @BeforeMethod
    public void setUp() {
	bucket = UtilsBucket.createTestBucket();
    }

    @Test(groups = { "fast" })
    public void createTestBucket_validArguments_createsExitingBucketDir() {
	assertTrue(bucket.getDirectory().exists());
	assertTrue(bucket.getDirectory().isDirectory());
    }

    public void createTestBucket_validArguments_createsWithIndexName() {
	String indexName = bucket.getIndex();
	assertTrue("Index name was " + indexName,
		indexName.matches("index-\\d*"));
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
	long earliest = 0;
	long latest = 0;
	try {
	    earliest = Long.parseLong(nameComponents[1]);
	    latest = Long.parseLong(nameComponents[2]);
	    Long.parseLong(nameComponents[3]);

	} catch (NumberFormatException e) {
	    UtilsTestNG.failForException("Couldn't parse the numbers", e);
	}
	assertTrue(earliest < latest);
    }

    public void createTestBucket_validArguments_createsWithSplunkFormat() {
	assertEquals(BucketFormat.SPLUNK_BUCKET, bucket.getFormat());
    }

    public void createTestBucketWithIndexAndName_validArguments_correctNameAndIndex() {
	String bucketName = "db_12351290_12351235_1";
	Bucket bucket = UtilsBucket.createTestBucketWithIndexAndName(
		"index-name", bucketName);
	assertEquals("index-name", bucket.getIndex());
	assertEquals(bucketName, bucket.getName());

    }

    public void createFileFormatedAsBucket_validArgument_theDirCanBeUsedToCreateABucket() {
	File bucketDir = UtilsBucket
		.createFileFormatedAsBucket("db_12351290_12351235_1");
	try {
	    new Bucket("index-name", bucketDir);
	} catch (Exception e) {
	    UtilsTestNG
		    .failForException("Coudn't create a valid bucket dir", e);
	}
    }

    public void createBucketInDirectory_givenDirectory_createsBucketInThatDirectory() {
	File parent = UtilsFile.createTempDirectory();
	Bucket bucketCreated = UtilsBucket.createBucketInDirectory(parent);
	assertEquals(parent, bucketCreated.getDirectory().getParentFile());
    }

    public void createBucketWithTimes_givenEarliestLatest_bucketNameStartsWith_db_earliest_latest() {
	Date earliest = new Date(12351235);
	Date latest = new Date(earliest.getTime() + 100);
	Bucket bucketWithTimes = UtilsBucket.createBucketWithTimes(earliest,
		latest);
	String expectedBucketNameStart = "db_" + latest.getTime() + "_"
		+ earliest.getTime();
	assertTrue(bucketWithTimes.getName()
		.startsWith(expectedBucketNameStart));
    }

    public void createBucketInDirectoryWithTimes_givenDirectory_createsBucketInTheDirectory() {
	File parent = null;
	try {
	    parent = createTempDirectory();
	    Bucket bucket = UtilsBucket.createBucketInDirectoryWithTimes(
		    parent, new Date(), new Date());
	    assertEquals(parent.getAbsolutePath(), bucket.getDirectory()
		    .getParentFile().getAbsolutePath());
	} finally {
	    FileUtils.deleteQuietly(parent);
	}
    }

    public void createBucketInDirectoryWithTimes_givenTimes_bucketNameStartsWith_db_earliest_latest() {
	File parent = null;
	try {
	    parent = createTempDirectory();
	    Date earliest = new Date();
	    Date latest = new Date();
	    Bucket bucketWithTimes = UtilsBucket
		    .createBucketInDirectoryWithTimes(parent, earliest, latest);
	    String expectedBucketNameStart = "db_" + earliest.getTime() + "_"
		    + latest.getTime();
	    assertTrue(bucketWithTimes.getName().startsWith(
		    expectedBucketNameStart));
	} finally {
	    FileUtils.deleteQuietly(parent);
	}
    }

    public void createBucketWithName_givenName_bucketWithName() {
	String name = "name";
	Bucket bucket = UtilsBucket.createBucketWithName(name);
	assertEquals(name, bucket.getName());
    }

    public void createBucketWithIndexAndTimeRange_givenParameters_bucketWithParameters() {
	String index = "index";
	Date earliest = new Date(12345678);
	Date latest = new Date(earliest.getTime() + 100);
	Bucket bucket = UtilsBucket.createBucketWithIndexAndTimeRange(index,
		earliest, latest);
	assertEquals(index, bucket.getIndex());
	assertEquals(earliest, bucket.getEarliest());
	assertEquals(latest, bucket.getLatest());
    }
}
