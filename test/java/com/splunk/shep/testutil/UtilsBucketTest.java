package com.splunk.shep.testutil;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.BucketFormat;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.model.FileNotDirectoryException;

@Test(groups = { "fast" })
public class UtilsBucketTest {

    File tempDirectory;

    @BeforeMethod(groups = { "fast" })
    public void setUp() {
	tempDirectory = UtilsFile.createTempDirectory();
    }

    @AfterMethod(groups = { "fast" })
    public void tearDown() throws IOException {
	FileUtils.deleteDirectory(tempDirectory);
    }

    public void createBucketDirectoriesInDirectory_defaultState_createBucketDirectoryWhichCanCreateABucketObject() {
	try {
	    createBucketWithUtilsBucket();
	} catch (Exception e) {
	    UtilsTestNG.failForException(
		    "Could not create Bucket with UtilsBucket", e);
	}
    }

    private Bucket createBucketWithUtilsBucket() throws FileNotFoundException,
	    FileNotDirectoryException {
	File bucketDir = UtilsBucket
		.createBucketDirectoriesInDirectory(tempDirectory);
	Bucket bucket = new Bucket(bucketDir);
	return bucket;
    }

    public void getIndex_createdBucketUsingCreateBucketInDirectory_matchBucketObjectsIndex()
	    throws FileNotFoundException, FileNotDirectoryException {
	Bucket bucket = createBucketWithUtilsBucket();
	assertEquals(bucket.getIndex(), UtilsBucket.getIndex());
    }

    public void getBucketName_createBucketUsingUtilsBucket_matchBucketObjectsName()
	    throws FileNotFoundException, FileNotDirectoryException {
	Bucket bucket = createBucketWithUtilsBucket();
	assertEquals(bucket.getName(), UtilsBucket.getBucketName());
    }

    public void createBucketInDirectory_withExistingDir_validIndexFormatAndBucketName() {
	Bucket bucket = UtilsBucket.createBucketInDirectory(tempDirectory);
	assertEquals(UtilsBucket.getIndex(), bucket.getIndex());
	assertEquals(UtilsBucket.getBucketName(), bucket.getName());
	assertEquals(BucketFormat.UNKNOWN, bucket.getFormat());
	assertTrue(bucket.getDirectory().exists());
    }

    public void createBucketPathInDirectory_defaultState_endsWithIndexDBAndBucketName() {
	String bucketPath = UtilsBucket
		.createBucketPathInDirectory(tempDirectory);
	String expectedBucketPathEnding = UtilsBucket.getIndex() + "/"
		+ UtilsBucket.getDB() + "/" + UtilsBucket.getBucketName();
	assertTrue(bucketPath.endsWith(expectedBucketPathEnding));
    }

    public void createBucketPathInDirectory_defaultState_canCreateBucketFromReturnedPath() {
	String createBucketPath = UtilsBucket
		.createBucketPathInDirectory(tempDirectory);
	try {
	    Bucket.createWithAbsolutePath(createBucketPath);
	} catch (Exception e) {
	    UtilsTestNG.failForException("Could not create bucket"
		    + " from UtilsBucket.getBucketPath", e);
	}
    }

    public void createBucketWithSplunkBucketFormatInDirectory_defaultState_createdBucketWithA_rawdata_DirectoryInsideTheBucket() {
	Bucket bucket = UtilsBucket
		.createBucketWithSplunkBucketFormatInDirectory(tempDirectory);
	assertEquals(BucketFormat.SPLUNK_BUCKET, bucket.getFormat());
    }
}
