// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.splunk.shuttl.archiver.importexport;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.util.UtilsFile;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public abstract class BucketFileCreatorTest {

	private BucketFileCreator bucketFileCreator;
	private Bucket bucket;
	private File dir;
	private File file;

	@BeforeMethod
	public void setUp() {
		dir = createDirectory();
		file = createFileInParent(dir, "testFile." + getExtension());
		bucket = TUtilsBucket.createBucket();
		bucketFileCreator = getInstance();
	}

	protected abstract BucketFormat getFormat();

	protected abstract String getExtension();

	protected abstract BucketFileCreator getInstance();

	@AfterMethod
	public void tearDown() {
		FileUtils.deleteQuietly(dir);
	}

	@Test(groups = { "fast-unit" })
	public void _givenFileAndBucket_bucketNamedAsFileWithoutTheExtension() {
		Bucket createdBucket = bucketFileCreator.createBucketWithFile(file, bucket);
		String fileWithoutExtension = UtilsFile.getFileNameSansExt(file,
				getExtension());
		assertEquals(createdBucket.getName(), fileWithoutExtension);
	}

	public void _givenFileAndBucket_newBucketObjectKeepsOldBucketSize() {
		Bucket newBucket = bucketFileCreator.createBucketWithFile(file, bucket);
		assertEquals(bucket.getSize(), newBucket.getSize());

	}

	public void _givenFile_createsBucketInParentFileToFile() {
		LocalBucket createdBucket = bucketFileCreator.createBucketWithFile(file,
				bucket);
		assertEquals(dir, createdBucket.getDirectory().getParentFile());
	}

	public void _givenBucket_bucketWithSameIndexAsGivenBucket() {
		Bucket createdBucket = bucketFileCreator.createBucketWithFile(file, bucket);
		assertEquals(bucket.getIndex(), createdBucket.getIndex());
	}

	public void _givenFile_bucketWithFormat() {
		Bucket createdBucket = bucketFileCreator.createBucketWithFile(file, bucket);
		assertEquals(getFormat(), createdBucket.getFormat());
	}

	public void _givenFile_fileMovedToBucketDirectory() {
		long fileSize = file.length();
		LocalBucket createdBucket = bucketFileCreator.createBucketWithFile(file,
				bucket);
		File bucketDir = createdBucket.getDirectory();
		assertEquals(1, bucketDir.listFiles().length);
		File movedFile = bucketDir.listFiles()[0];
		assertEquals(file.getName(), movedFile.getName());
		assertEquals(fileSize, movedFile.length());
		assertFalse(file.exists());
	}

	public void _givenFile_bucketDirectoryHasSameNameAsFileWithoutExtension() {
		LocalBucket createdBucket = bucketFileCreator.createBucketWithFile(file,
				bucket);
		File bucketDir = createdBucket.getDirectory();
		String fileWithoutExtension = UtilsFile.getFileNameSansExt(file,
				getExtension());
		assertEquals(fileWithoutExtension, bucketDir.getName());
	}

	// Sad path

	@Test(expectedExceptions = { IllegalArgumentException.class })
	public void _givenFileWithOtherExtension_throwIllegalArgumentException() {
		String otherExtension = getExtension() + "x";
		File otherExtensionedFile = createTestFileWithName("file." + otherExtension);
		assertNotEquals(getExtension(),
				FilenameUtils.getExtension(otherExtensionedFile.getName()));
		bucketFileCreator.createBucketWithFile(otherExtensionedFile, null);
	}

	@Test(expectedExceptions = { BucketFileNotFoundException.class })
	public void _givenNonExistantFile_throwBucketFileNotFoundException() {
		assertTrue(file.delete());
		assertFalse(file.exists());
		bucketFileCreator.createBucketWithFile(file, bucket);
	}
}
