package com.splunk.shep.archiver.model;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.splunk.shep.testutil.UtilsFile;

@Test(groups = { "fast" })
public class BucketTest {

    File rootTestDirectory;

    @AfterMethod(groups = { "fast" })
    public void tearDown() throws IOException {
	if (rootTestDirectory != null)
	    FileUtils.deleteDirectory(rootTestDirectory);
    }

    public void constructor_takingAbsolutePathToABucket_setIndex()
	    throws FileNotFoundException {
	String index = "index";
	String bucketPath = getBucketPathWithIndex(index);
	Bucket bucket = Bucket.createWithAbsolutePath(bucketPath);
	assertEquals(index, bucket.getIndex());
    }

    private String getBucketPathWithIndex(String index) {
	rootTestDirectory = UtilsFile.createTempDirectory();
	File indexDir = UtilsFile.createDirectoryInParent(rootTestDirectory,
		index);
	File dbDir = UtilsFile.createDirectoryInParent(indexDir, "db");
	File bucketDir = UtilsFile.createDirectoryInParent(dbDir,
		"db_1326857236_1300677707_0");
	return bucketDir.getAbsolutePath();
    }

    public void constructor_absolutePathToBucketEndingWithSlash_setIndex()
	    throws FileNotFoundException {
	String index = "index";
	String bucketPath = getBucketPathWithIndex(index) + "/";
	Bucket bucket = Bucket.createWithAbsolutePath(bucketPath);
	assertEquals(index, bucket.getIndex());
    }

    public void createWithAbsolutePath_takingStringToAnExistingDirectory_notNullBucket()
	    throws FileNotFoundException {
	File tempDir = UtilsFile.createTempDirectory();
	assertNotNull(Bucket.createWithAbsolutePath(tempDir.getAbsolutePath()));
    }

    @Test(expectedExceptions = { FileNotFoundException.class })
    public void createWithAbsolutePath_takingStringToNonExistingDirectory_throwFileNotFoundException()
	    throws FileNotFoundException {
	File nonExistingFile = new File("does-not-exist");
	assertTrue(!nonExistingFile.exists());
	Bucket.createWithAbsolutePath(nonExistingFile.getAbsolutePath());
    }

    public void BucketTest_getBucketPathWithIndex_withNonEmptyIndex_endsWithExpectedPathEnding() {
	String index = "someIndex";
	String bucketPath = getBucketPathWithIndex(index);
	String expectedBucketPathEnding = index
		+ "/db/db_1326857236_1300677707_0";
	assertTrue(bucketPath.endsWith(expectedBucketPathEnding));
    }
}
