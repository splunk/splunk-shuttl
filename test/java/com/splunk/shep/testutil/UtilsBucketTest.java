package com.splunk.shep.testutil;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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

    public void createBucketInDirectory_defaultState_createBucketDirectoryWhichCanCreateABucketObject() {
	try {
	    createBucketWithUtilsBucket();
	} catch (Exception e) {
	    UtilsTestNG.failForException(
		    "Could not create Bucket with UtilsBucket", e);
	}
    }

    private Bucket createBucketWithUtilsBucket() throws FileNotFoundException,
	    FileNotDirectoryException {
	File bucketDir = UtilsBucket.createBucketInDirectory(tempDirectory);
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
}
