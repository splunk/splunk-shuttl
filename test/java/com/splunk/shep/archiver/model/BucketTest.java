package com.splunk.shep.archiver.model;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.BucketFormat;
import com.splunk.shep.testutil.UtilsFile;
import com.splunk.shep.testutil.UtilsTestNG;

@Test(groups = { "fast" })
public class BucketTest {

    File rootTestDirectory;
    String index = "index";

    @AfterMethod(groups = { "fast" })
    public void tearDown() throws IOException {
	if (rootTestDirectory != null)
	    FileUtils.deleteDirectory(rootTestDirectory);
    }

    public void constructor_takingAbsolutePathToABucket_setIndex()
	    throws IOException {
	String bucketPath = getBucketPathWithIndex();
	Bucket bucket = Bucket.createWithAbsolutePath(bucketPath);
	assertEquals(index, bucket.getIndex());
    }

    private String getBucketPathWithIndex() {
	return getBucketDirectoryWithIndex(index).getAbsolutePath();
    }

    private File getBucketDirectoryWithIndex(String index) {
	rootTestDirectory = UtilsFile.createTempDirectory();
	File indexDir = UtilsFile.createDirectoryInParent(rootTestDirectory,
		index);
	File dbDir = UtilsFile.createDirectoryInParent(indexDir, "db");
	File bucketDir = UtilsFile.createDirectoryInParent(dbDir,
		getBucketName());
	return bucketDir;
    }

    private String getBucketName() {
	return "db_1326857236_1300677707_0";
    }

    public void constructor_absolutePathToBucketEndingWithSlash_setIndex()
	    throws IOException {
	String bucketPath = getBucketPathWithIndex() + "/";
	Bucket bucket = Bucket.createWithAbsolutePath(bucketPath);
	assertEquals(index, bucket.getIndex());
    }

    public void createWithAbsolutePath_takingStringToAnExistingDirectory_notNullBucket()
	    throws IOException {
	File tempDir = UtilsFile.createTempDirectory();
	assertNotNull(Bucket.createWithAbsolutePath(tempDir.getAbsolutePath()));
    }

    @Test(expectedExceptions = { FileNotFoundException.class })
    public void createWithAbsolutePath_takingStringToNonExistingDirectory_throwFileNotFoundException()
	    throws IOException {
	File nonExistingFile = new File("does-not-exist");
	assertTrue(!nonExistingFile.exists());
	Bucket.createWithAbsolutePath(nonExistingFile.getAbsolutePath());
    }

    @Test(expectedExceptions = { FileNotDirectoryException.class })
    public void createWithAbsolutePath_wherePathIsAFileNotADirectory_throwFileNotDirectoryException()
	    throws IOException {
	File file = UtilsFile.createTestFile();
	assertTrue(file.isFile());
	Bucket.createWithAbsolutePath(file.getAbsolutePath());
    }

    public void createWithAbsolutePath_rawdataDirectoryExistsInsideBucket_getFormatReturnsSplunkBucket()
	    throws IOException {
	File bucketDir = getBucketDirectoryWithIndex(index);
	File rawdata = UtilsFile.createDirectoryInParent(bucketDir, "rawdata");
	assertTrue(rawdata.exists());
	Bucket bucket = Bucket.createWithAbsolutePath(bucketDir
		.getAbsolutePath());
	assertEquals(bucket.getFormat(), BucketFormat.SPLUNK_BUCKET);
    }

    /**
     * Until We've implemented more bucket formats, this is what happens.<br/>
     * This test should probably be removed when we get more formats.
     */
    public void createWithAbsolutePath_rawdataNotInBucket_bucketFormatIsUnknown()
	    throws IOException {
	Bucket bucket = Bucket.createWithAbsolutePath(getBucketPathWithIndex());
	assertEquals(BucketFormat.UNKNOWN, bucket.getFormat());
    }

    public void createWithAbsolutePath_givenExistingDirectory_bucketNameIsLastDirectoryInPath()
	    throws IOException {
	String bucketPath = getBucketPathWithIndex();
	String expectedName = getBucketName();
	Bucket bucket = Bucket.createWithAbsolutePath(bucketPath);
	assertEquals(expectedName, bucket.getName());
    }

    public void createWithAbsolutePath_givenExistingDirectory_getDirectoryShouldReturnThatDirectoryWithTheSameAbsolutePath()
	    throws IOException {
	File existingDirectory = getBucketDirectoryWithIndex(index);
	assertTrue(existingDirectory.exists());
	Bucket bucket = Bucket.createWithAbsolutePath(existingDirectory
		.getAbsolutePath());
	assertEquals(existingDirectory.getAbsolutePath(), bucket.getDirectory()
		.getAbsolutePath());
    }

    public void moveBucket_givenExistingDirectory_keepIndexFormatAndBucketName()
	    throws IOException {
	Bucket bucket = getValidBucket();
	File directoryToMoveTo = UtilsFile.createTempDirectory();
	Bucket movedBucket = bucket.moveBucketToDir(directoryToMoveTo);
	boolean isMovedBucketAChildOfDirectoryMovedTo = movedBucket
		.getDirectory().getAbsolutePath()
		.contains(directoryToMoveTo.getAbsolutePath());
	assertTrue(isMovedBucketAChildOfDirectoryMovedTo);

	// Sanity checks.
	assertEquals(bucket.getIndex(), movedBucket.getIndex());
	assertEquals(bucket.getFormat(), movedBucket.getFormat());
	assertEquals(bucket.getName(), movedBucket.getName());
	Assert.assertNotEquals(bucket.getDirectory().getAbsolutePath(),
		movedBucket.getDirectory().getAbsolutePath());

	// TearDown
	FileUtils.deleteDirectory(directoryToMoveTo);
    }

    public void moveBucket_givenExistingDirectory_removeOldBucket()
	    throws IOException {
	Bucket bucket = getValidBucket();
	File directoryToMoveTo = UtilsFile.createTempDirectory();
	bucket.moveBucketToDir(directoryToMoveTo);
	assertTrue(!bucket.getDirectory().exists());

	FileUtils.deleteDirectory(directoryToMoveTo);
    }

    private Bucket getValidBucket() {
	File existingDir = getBucketDirectoryWithIndex(index);
	return getBucketWithDir(existingDir);
    }

    private Bucket getBucketWithDir(File existingDir) {
	try {
	    return new Bucket(existingDir);
	} catch (IOException e) {
	    UtilsTestNG.failForException("Could not create "
		    + "bucket with directory: " + existingDir, e);
	    return null;
	}
    }

    public void BucketTest_getValidBucket_perDefault_validIndexFormatAndBucketName() {
	Bucket bucket = getValidBucket();
	assertEquals(index, bucket.getIndex());
	assertEquals(getBucketName(), bucket.getName());
	assertEquals(BucketFormat.UNKNOWN, bucket.getFormat());
	assertTrue(bucket.getDirectory().exists());
    }

    public void BucketTest_getBucketPathWithIndex_withNonEmptyIndex_endsWithExpectedPathEnding() {
	String bucketPath = getBucketPathWithIndex();
	String expectedBucketPathEnding = index
		+ "/db/db_1326857236_1300677707_0";
	assertTrue(bucketPath.endsWith(expectedBucketPathEnding));
    }
}
