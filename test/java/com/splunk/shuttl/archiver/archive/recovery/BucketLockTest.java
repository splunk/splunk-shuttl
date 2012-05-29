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
package com.splunk.shuttl.archiver.archive.recovery;

import static com.splunk.shuttl.archiver.LocalFileSystemConstants.*;
import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketLockTest {

	File testRootDirectory;
	File locksDirectory;
	Bucket bucket;
	BucketLock bucketLock;

	@BeforeMethod
	public void setUp() {
		testRootDirectory = createDirectory();
		locksDirectory = createDirectory();
		bucket = TUtilsBucket.createBucketInDirectory(testRootDirectory);
		bucketLock = new BucketLock(bucket, locksDirectory);
	}

	@AfterMethod
	public void tearDown() throws IOException {
		FileUtils.deleteDirectory(testRootDirectory);
		FileUtils.deleteDirectory(locksDirectory);
	}

	@AfterTest
	public void deleteDefaultBucketLockDirectory() throws IOException {
		FileUtils.deleteDirectory(getArchiverDirectory());
	}

	@Test(groups = { "fast-unit" })
	public void getLockFile_createdWithBucket_lockFilesNameIncludesBucketsNameForUniqueness() {
		File lockFile = bucketLock.getLockFile();
		assertTrue(lockFile.getName().contains(bucket.getName()));
	}

	public void getLockFile_createdWithLocksDirectory_lockFileIsInTheLocksDirectory() {
		File lockFile = bucketLock.getLockFile();
		assertEquals(locksDirectory.getAbsolutePath(), lockFile.getParentFile()
				.getAbsolutePath());
	}

	public void deleteLockFile_bucketNotLocked_true() {
		bucketLock.deleteLockFile();
		assertFalse(bucketLock.getLockFile().exists());
	}

	public void deleteLockFile_bucketLocked_true() {
		assertTrue(bucketLock.tryLockExclusive());
		bucketLock.deleteLockFile();
		assertFalse(bucketLock.getLockFile().exists());
	}
}
