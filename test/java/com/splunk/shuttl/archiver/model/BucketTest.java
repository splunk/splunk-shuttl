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
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.FileUtils;
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

	private Bucket newBucket(String index, String path) throws IOException {
		return newBucket(index, new File(path));
	}

	private Bucket newBucket(String index, File file) throws IOException {
		return new Bucket(index, file, BucketFormat.SPLUNK_BUCKET);
	}

	@Test(groups = { "fast-unit" })
	public void getIndex_validArguments_correctIndexName() throws IOException {
		File file = TUtilsFile.createDirectory();
		Bucket bucket = newBucket("index-name", file);
		assertEquals("index-name", bucket.getIndex());
	}

	public void getIndex_absolutePathToBucketEndingWithSlash_correctIndexName()
			throws IOException {
		File file = TUtilsFile.createDirectory();
		Bucket bucket = newBucket("index-name", file.getAbsolutePath() + "/");
		assertEquals("index-name", bucket.getIndex());
	}

	public void createWithAbsolutePath_takingStringToAnExistingDirectory_notNullBucket()
			throws IOException {
		File tempDir = TUtilsFile.createDirectory();
		assertNotNull(newBucket("indexName", tempDir));
	}

	@Test(expectedExceptions = { FileNotFoundException.class })
	public void createWithAbsolutePath_takingStringToNonExistingDirectory_throwFileNotFoundException()
			throws IOException {
		File nonExistingFile = new File("does-not-exist");
		assertTrue(!nonExistingFile.exists());
		newBucket("index-name", nonExistingFile);
	}

	@Test(expectedExceptions = { FileNotDirectoryException.class })
	public void createWithAbsolutePath_wherePathIsAFileNotADirectory_throwFileNotDirectoryException()
			throws IOException {
		File file = TUtilsFile.createFile();
		assertTrue(file.isFile());
		newBucket("index-name", file);
	}

	public void createWithAbsolutePath_rawdataDirectoryExistsInsideBucket_getFormatReturnsSplunkBucket()
			throws IOException {
		Bucket fakeBucket = TUtilsBucket.createBucket();
		Bucket bucket = newBucket("index-name", fakeBucket.getDirectory());
		assertEquals(bucket.getFormat(), BucketFormat.SPLUNK_BUCKET);
	}

	public void getFormat_createWithFormat_getsFormat() throws IOException {
		BucketFormat format = BucketFormat.UNKNOWN;
		Bucket bucket = new Bucket("index-name", TUtilsFile.createDirectory(),
				format);
		assertEquals(format, bucket.getFormat());
	}

	public void getName_givenExistingDirectory_correctBucketName()
			throws IOException {
		Bucket fakeBucket = TUtilsBucket.createBucketWithIndexAndName("index-name",
				"db_12351235_12351290_1");
		Bucket bucket = newBucket("index-name", fakeBucket.getDirectory());
		assertEquals("db_12351235_12351290_1", bucket.getName());

	}

	public void createWithAbsolutePath_givenExistingDirectory_getDirectoryShouldReturnThatDirectoryWithTheSameAbsolutePath()
			throws IOException {
		File existingDirectory = TUtilsBucket
				.createFileFormatedAsBucket("db_12351235_12351290_1");
		Bucket bucket = newBucket("index-name", existingDirectory);
		assertEquals(existingDirectory.getAbsolutePath(), bucket.getDirectory()
				.getAbsolutePath());
	}

	public void deleteBucket_createdValidBucket_bucketRemovedFromFileSystem()
			throws IOException {
		Bucket createdBucket = TUtilsBucket.createBucket();
		createdBucket.deleteBucket();
		assertTrue(!createdBucket.getDirectory().exists());
	}

	public void deleteBucket_createdValidBucket_onlyBucketFolderRemovedFromFileSystem()
			throws IOException {
		Bucket createdBucket = TUtilsBucket.createBucket();
		File bucketParent = createdBucket.getDirectory().getParentFile();
		createdBucket.deleteBucket();
		assertTrue(!createdBucket.getDirectory().exists());
		assertTrue(bucketParent.exists());
	}

	public void equals_givenTwoBucketsCreatedWithSameIndexAndSameAbsolutePath_equalEachother()
			throws IOException {
		Bucket testBucket = TUtilsBucket.createBucket();
		String index = testBucket.getIndex();
		String absolutePath = testBucket.getDirectory().getAbsolutePath();

		Bucket bucket1 = newBucket(index, absolutePath);
		Bucket bucket2 = newBucket(index, absolutePath);
		assertEquals(bucket1, bucket2);
	}

	public void getURI_validBucket_notNullURI() {
		Bucket bucket = TUtilsBucket.createBucket();
		assertNotNull(bucket.getURI());
	}

	public void getDirectory_initWithFileUri_getDirectory() throws IOException {
		File file = createDirectory();
		Bucket bucketWithFileUri = new Bucket(file.toURI(), null, null, null);
		assertEquals(file.getAbsolutePath(), bucketWithFileUri.getDirectory()
				.getAbsolutePath());
	}

	@Test(expectedExceptions = { FileNotFoundException.class })
	public void uriConstructor_initWithFileUriToNonExistingDirectory_throwsFileNotFoundException()
			throws IOException {
		File file = createDirectory();
		assertTrue(file.delete());
		new Bucket(file.toURI(), null, null, null);
	}

	@Test(expectedExceptions = { FileNotDirectoryException.class })
	public void uriConstructor_initWithFileUriToFileInsteadOfDirectory_throwsFileNotDirectoryException()
			throws IOException {
		File file = createFile();
		new Bucket(file.toURI(), null, null, null);
	}

	public void isRemote_uriConstructorWithLocalFileAsUri_false()
			throws IOException {
		URI localFileUri = createDirectory().toURI();
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

	public void getSize_localBucket_notNull() throws IOException {
		Bucket bucket = new Bucket(createDirectory().toURI(), null, null, null);
		assertNotNull(bucket.getSize());
	}

	public void getSize_remoteBucket_null() throws IOException {
		Bucket bucket = new Bucket(URI.create("remote:/bucket"), null, null, null);
		assertNull(bucket.getSize());
	}


}
