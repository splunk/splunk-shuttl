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

package com.splunk.shuttl.archiver.model;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFile;

@Test(groups = { "fast-unit" })
public class BucketTest {

    File rootTestDirectory;

    @AfterMethod(groups = { "fast-unit" })
    public void tearDown() throws IOException {
	if (rootTestDirectory != null)
	    FileUtils.deleteDirectory(rootTestDirectory);
    }

    @Test(groups = { "fast-unit" })
    public void getIndex_validArguments_correctIndexName() throws IOException {
	File file = TUtilsFile.createTempDirectory();
	Bucket bucket = new Bucket("index-name", file);
	assertEquals("index-name", bucket.getIndex());
    }

    public void getIndex_absolutePathToBucketEndingWithSlash_correctIndexName()
	    throws IOException {
	File file = TUtilsFile.createTempDirectory();
	Bucket bucket = new Bucket("index-name", file.getAbsolutePath() + "/");
	assertEquals("index-name", bucket.getIndex());
    }

    public void createWithAbsolutePath_takingStringToAnExistingDirectory_notNullBucket()
	    throws IOException {
	File tempDir = TUtilsFile.createTempDirectory();
	assertNotNull(new Bucket("indexName", tempDir));
    }

    @Test(expectedExceptions = { FileNotFoundException.class })
    public void createWithAbsolutePath_takingStringToNonExistingDirectory_throwFileNotFoundException()
	    throws IOException {
	File nonExistingFile = new File("does-not-exist");
	assertTrue(!nonExistingFile.exists());
	new Bucket("index-name", nonExistingFile);
    }

    @Test(expectedExceptions = { FileNotDirectoryException.class })
    public void createWithAbsolutePath_wherePathIsAFileNotADirectory_throwFileNotDirectoryException()
	    throws IOException {
	File file = TUtilsFile.createTestFile();
	assertTrue(file.isFile());
	new Bucket("index-name", file);
    }

    public void createWithAbsolutePath_rawdataDirectoryExistsInsideBucket_getFormatReturnsSplunkBucket()
	    throws IOException {
	Bucket fakeBucket = TUtilsBucket.createTestBucket();
	Bucket bucket = new Bucket("index-name", fakeBucket.getDirectory());
	assertEquals(bucket.getFormat(), BucketFormat.SPLUNK_BUCKET);
    }

    /**
     * Until We've implemented more bucket formats, this is what happens.<br/>
     * This test should probably be removed when we get more formats.
     */
    public void createWithAbsolutePath_rawdataNotInBucket_bucketFormatIsUnknown()
	    throws IOException {
	File file = TUtilsFile.createTempDirectory();
	Bucket bucket = new Bucket("index-name", file);
	assertEquals(BucketFormat.UNKNOWN, bucket.getFormat());
    }

    public void getName_givenExistingDirectory_correctBucketName()
	    throws IOException {
	Bucket fakeBucket = TUtilsBucket.createTestBucketWithIndexAndName(
		"index-name", "db_12351235_12351290_1");
	Bucket bucket = new Bucket("index-name", fakeBucket.getDirectory());
	assertEquals("db_12351235_12351290_1", bucket.getName());

    }

    public void createWithAbsolutePath_givenExistingDirectory_getDirectoryShouldReturnThatDirectoryWithTheSameAbsolutePath()
	    throws IOException {
	File existingDirectory = TUtilsBucket
		.createFileFormatedAsBucket("db_12351235_12351290_1");
	Bucket bucket = new Bucket("index-name", existingDirectory);
	assertEquals(existingDirectory.getAbsolutePath(), bucket.getDirectory()
		.getAbsolutePath());
    }

    public void moveBucket_givenExistingDirectory_keepIndexFormatAndBucketName()
	    throws IOException {
	Bucket bucket = TUtilsBucket.createTestBucket();
	File directoryToMoveTo = TUtilsFile.createTempDirectory();

	Bucket movedBucket = bucket.moveBucketToDir(directoryToMoveTo);

	boolean isMovedBucketAChildOfDirectoryMovedTo = movedBucket
		.getDirectory().getAbsolutePath()
		.contains(directoryToMoveTo.getAbsolutePath());
	assertTrue(isMovedBucketAChildOfDirectoryMovedTo);

	// Sanity checks.
	Assert.assertEquals(movedBucket.getName(), bucket.getName());
	Assert.assertEquals(movedBucket.getIndex(), bucket.getIndex());
	Assert.assertTrue(!bucket.getDirectory().getAbsolutePath()
		.equals(movedBucket.getDirectory().getAbsolutePath()));

	// Teardown
	FileUtils.deleteDirectory(directoryToMoveTo);
    }

    public void moveBucket_givenExistingDirectory_removeOldBucket()
	    throws IOException {
	Bucket bucket = TUtilsBucket.createTestBucket();
	File directoryToMoveTo = TUtilsFile.createTempDirectory();
	File oldPath = new File(bucket.getDirectory().getAbsolutePath());
	bucket.moveBucketToDir(directoryToMoveTo);

	Assert.assertFalse(oldPath.exists());

	// Teardown
	FileUtils.deleteDirectory(directoryToMoveTo);
    }

    @Test(expectedExceptions = { FileNotDirectoryException.class })
    public void moveBucket_givenNonDirectory_throwFileNotDirectoryException() {
	File file = TUtilsFile.createTestFile();
	TUtilsBucket.createTestBucket().moveBucketToDir(file);
    }

    @Test(expectedExceptions = { DirectoryDidNotExistException.class })
    public void moveBucket_givenNonExistingDirectory_throwFileNotFoundException() {
	File nonExistingDir = new File("non-existing-dir");
	nonExistingDir.mkdirs();
	FileUtils.deleteQuietly(nonExistingDir);
	TUtilsBucket.createTestBucket().moveBucketToDir(nonExistingDir);
    }

    public void moveBucket_givenDirectoryWithContents_contentShouldBeMoved()
	    throws IOException {
	// Setup
	Bucket bucket = TUtilsBucket.createTestBucket();
	String contentsFileName = "contents";
	// Creation
	File contents = TUtilsFile.createFileInParent(bucket.getDirectory(),
		contentsFileName);
	TUtilsFile.populateFileWithRandomContent(contents);
	File directoryToMoveTo = TUtilsFile.createTempDirectory();
	List<String> contentLines = IOUtils.readLines(new FileReader(contents));

	// Test
	Bucket movedBucket = bucket.moveBucketToDir(directoryToMoveTo);
	File movedContents = new File(movedBucket.getDirectory()
		.getAbsolutePath(), contentsFileName);
	assertTrue(movedContents.exists());
	assertTrue(!contents.exists());
	List<String> movedContentLines = IOUtils.readLines(new FileReader(
		movedContents));
	assertEquals(contentLines, movedContentLines);

	// Teardown
	FileUtils.deleteDirectory(directoryToMoveTo);
    }

    public void deleteBucket_createdValidBucket_bucketRemovedFromFileSystem()
	    throws IOException {
	Bucket createdBucket = TUtilsBucket.createTestBucket();
	createdBucket.deleteBucket();
	assertTrue(!createdBucket.getDirectory().exists());
    }

    public void deleteBucket_createdValidBucket_onlyBucketFolderRemovedFromFileSystem()
	    throws IOException {
	Bucket createdBucket = TUtilsBucket.createTestBucket();
	File bucketParent = createdBucket.getDirectory().getParentFile();
	createdBucket.deleteBucket();
	assertTrue(!createdBucket.getDirectory().exists());
	assertTrue(bucketParent.exists());
    }

    public void equals_givenTwoBucketsCreatedWithSameIndexAndSameAbsolutePath_equalEachother()
	    throws IOException {
	Bucket testBucket = TUtilsBucket.createTestBucket();
	String index = testBucket.getIndex();
	String absolutePath = testBucket.getDirectory().getAbsolutePath();

	Bucket bucket1 = new Bucket(index, absolutePath);
	Bucket bucket2 = new Bucket(index, absolutePath);
	assertEquals(bucket1, bucket2);
    }

    public void getFormat_afterHaveBeingMoved_returnTheSameValueAsBeforeTheMove()
	    throws IOException {
	rootTestDirectory = createTempDirectory();
	Bucket bucket = TUtilsBucket.createTestBucket();
	BucketFormat format = bucket.getFormat();
	bucket.moveBucketToDir(rootTestDirectory);
	assertEquals(format, bucket.getFormat());
    }

    public void getURI_validBucket_notNullURI() {
	Bucket bucket = TUtilsBucket.createTestBucket();
	assertNotNull(bucket.getURI());
    }

    public void getDirectory_initWithFileUri_getDirectory() throws IOException {
	File file = createTempDirectory();
	Bucket bucketWithFileUri = new Bucket(file.toURI(), null, null, null);
	assertEquals(file.getAbsolutePath(), bucketWithFileUri.getDirectory()
		.getAbsolutePath());
    }

    @Test(expectedExceptions = { FileNotFoundException.class })
    public void uriConstructor_initWithFileUriToNonExistingDirectory_throwsFileNotFoundException()
	    throws IOException {
	File file = createTempDirectory();
	assertTrue(file.delete());
	new Bucket(file.toURI(), null, null, null);
    }

    @Test(expectedExceptions = { FileNotDirectoryException.class })
    public void uriConstructor_initWithFileUriToFileInsteadOfDirectory_throwsFileNotDirectoryException()
	    throws IOException {
	File file = createTestFile();
	new Bucket(file.toURI(), null, null, null);
    }

    public void isRemote_uriConstructorWithLocalFileAsUri_false()
	    throws IOException {
	URI localFileUri = createTempDirectory().toURI();
	assertFalse(new Bucket(localFileUri, null, null, null).isRemote());
    }

    public void init_withNullURI_shouldBePossibleForTestableCreationOfBuckets()
	    throws FileNotFoundException, FileNotDirectoryException {
	new Bucket(null, "index", "bucketName", BucketFormat.UNKNOWN);
    }

    public void init_withNonFileURI_shouldBePossibleForTestableCreationOfBuckets()
	    throws FileNotFoundException, FileNotDirectoryException {
	new Bucket(URI.create("valid:/uri"), "index", "bucketName",
		BucketFormat.UNKNOWN);
    }

}
