package com.splunk.shep.testutil;

import static org.testng.AssertJUnit.*;

import java.io.File;

import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.BucketFormat;
import com.splunk.shep.archiver.model.Bucket;

@Test(groups = { "fast" })
public class UtilsBucketTest {

    public void createTestBucket_validArguments_createsExitingBucketDir() {
	Bucket bucket = UtilsBucket.createTestBucket();
	assertTrue(bucket.getDirectory().exists());
	assertTrue(bucket.getDirectory().isDirectory());
    }

    public void createTestBucket_validArguments_createsWithIndexName() {
	Bucket bucket = UtilsBucket.createTestBucket();
	String indexName = bucket.getIndex();
	assertTrue("Index name was " + indexName,
		indexName.matches("index-\\d*"));
    }

    public void createTestBucket_validArguments_createsWithRawdataDir() {
	Bucket bucket = UtilsBucket.createTestBucket();

	File rawDataDir = new File(bucket.getDirectory(), "rawdata");
	assertTrue(rawDataDir.exists());
	assertTrue(rawDataDir.isDirectory());
    }

    public void createTestBucket_validArguments_createsWithValidBucketName() {
	Bucket bucket = UtilsBucket.createTestBucket();

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
	Bucket bucket = UtilsBucket.createTestBucket();

	assertEquals(BucketFormat.SPLUNK_BUCKET, bucket.getFormat());
    }

    public void createTestBucketWithIndexAndName_validArguments_correctNameAndIndex() {
	Bucket bucket = UtilsBucket.createTestBucketWithIndexAndName(
		"index-name", "db_12351235_12351290_1");
	assertEquals("index-name", bucket.getIndex());
	assertEquals("db_12351235_12351290_1", bucket.getName());

    }

    public void createFileFormatedAsBucket_validArgument_theDirCanBeUsedToCreateABucket() {
	File bucketDir = UtilsBucket
		.createFileFormatedAsBucket("db_12351235_12351290_1");
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
}
