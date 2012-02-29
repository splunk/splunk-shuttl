package com.splunk.shep.archiver.model;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.BucketFormat;
import com.splunk.shep.testutil.UtilsBucket;
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
	    throws IOException {
	String bucketPath = createBucketPath();
	Bucket bucket = Bucket.createWithAbsolutePath(bucketPath);
	assertEquals(UtilsBucket.getIndex(), bucket.getIndex());
    }

    private String createBucketPath() {
	return UtilsBucket.createBucketDirectoriesInDirectory(
		createRootTestDirectory()).getAbsolutePath();
    }

    private File createRootTestDirectory() {
	rootTestDirectory = UtilsFile.createTempDirectory();
	return rootTestDirectory;
    }

    private Bucket createBucket() {
	return UtilsBucket.createBucketInDirectory(createRootTestDirectory());
    }

    public void constructor_absolutePathToBucketEndingWithSlash_setIndex()
	    throws IOException {
	String bucketPath = createBucketPath() + "/";
	Bucket bucket = Bucket.createWithAbsolutePath(bucketPath);
	assertEquals(UtilsBucket.getIndex(), bucket.getIndex());
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
	File bucketDir = UtilsBucket
		.createBucketDirectoriesInDirectory(createRootTestDirectory());
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
	Bucket bucket = Bucket.createWithAbsolutePath(createBucketPath());
	assertEquals(BucketFormat.UNKNOWN, bucket.getFormat());
    }

    public void createWithAbsolutePath_givenExistingDirectory_bucketNameIsLastDirectoryInPath()
	    throws IOException {
	String bucketPath = createBucketPath();
	String expectedName = UtilsBucket.getBucketName();
	Bucket bucket = Bucket.createWithAbsolutePath(bucketPath);
	assertEquals(expectedName, bucket.getName());
    }

    public void createWithAbsolutePath_givenExistingDirectory_getDirectoryShouldReturnThatDirectoryWithTheSameAbsolutePath()
	    throws IOException {
	File existingDirectory = UtilsBucket
		.createBucketDirectoriesInDirectory(createRootTestDirectory());
	assertTrue(existingDirectory.exists());
	Bucket bucket = Bucket.createWithAbsolutePath(existingDirectory
		.getAbsolutePath());
	assertEquals(existingDirectory.getAbsolutePath(), bucket.getDirectory()
		.getAbsolutePath());
    }

    public void moveBucket_givenExistingDirectory_keepIndexFormatAndBucketName()
	    throws IOException {
	Bucket bucket = createBucket();
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
	Bucket bucket = createBucket();
	File directoryToMoveTo = UtilsFile.createTempDirectory();
	bucket.moveBucketToDir(directoryToMoveTo);
	assertTrue(!bucket.getDirectory().exists());

	FileUtils.deleteDirectory(directoryToMoveTo);
    }

    @Test(expectedExceptions = { FileNotDirectoryException.class })
    public void moveBucket_givenNonDirectory_throwFileNotDirectoryException()
	    throws FileNotFoundException, FileNotDirectoryException {
	File file = UtilsFile.createTestFile();
	Bucket bucket = createBucket();
	bucket.moveBucketToDir(file);
    }

    @Test(expectedExceptions = { FileNotFoundException.class })
    public void moveBucket_givenNonExistingDirectory_throwFileNotFoundException()
	    throws FileNotFoundException, FileNotDirectoryException {
	File nonExistingDir = new File("non-existing-dir");
	createBucket().moveBucketToDir(nonExistingDir);
    }

    public void moveBucket_givenDirectoryWithContents_contentShouldBeMoved()
	    throws IOException {
	// Setup
	Bucket bucket = createBucket();
	String contentsFileName = "contents";
	// Creation
	File contents = UtilsFile.createFileInParent(bucket.getDirectory(),
		contentsFileName);
	UtilsFile.populateFileWithRandomContent(contents);
	File directoryToMoveTo = UtilsFile.createTempDirectory();
	List<String> contentLines = IOUtils.readLines(new FileReader(contents));

	// Test
	Bucket movedBucket = bucket.moveBucketToDir(directoryToMoveTo);
	File movedContents = new File(movedBucket.getDirectory(),
		contentsFileName);
	assertTrue(movedContents.exists());
	assertTrue(!contents.exists());
	List<String> movedContentLines = IOUtils.readLines(new FileReader(
		movedContents));
	assertEquals(contentLines, movedContentLines);

	// Teardown
	FileUtils.deleteDirectory(directoryToMoveTo);
    }

}
