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
package com.splunk.shuttl.archiver.model;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFile;

@Test(groups = { "fast-unit" })
public class MovesBucketsTest {

	private Bucket bucket;
	private File directoryToMoveTo;

	@BeforeMethod
	public void setUp() {
		bucket = TUtilsBucket.createBucket();
		directoryToMoveTo = TUtilsFile.createDirectory();
	}

	@AfterMethod
	public void tearDown() {
		FileUtils.deleteQuietly(directoryToMoveTo);
	}

	@Test(groups = { "fast-unit" })
	public void moveBucket_givenSetUp_movedBucketsParentIsTheDirectoryToMoveTo() {
		Bucket movedBucket = MovesBuckets.moveBucket(bucket, directoryToMoveTo);
		assertEquals(directoryToMoveTo.getAbsolutePath(), movedBucket
				.getDirectory().getParentFile().getAbsolutePath());
	}

	public void moveBucket_givenExistingDirectory_keepIndexFormatAndBucketName()
			throws IOException {
		Bucket movedBucket = MovesBuckets.moveBucket(bucket, directoryToMoveTo);

		assertEquals(movedBucket.getName(), bucket.getName());
		assertEquals(movedBucket.getIndex(), bucket.getIndex());
		assertTrue(!bucket.getDirectory().getAbsolutePath()
				.equals(movedBucket.getDirectory().getAbsolutePath()));
	}

	public void moveBucket_givenExistingDirectory_removeOldBucket()
			throws IOException {
		assertTrue(bucket.getDirectory().exists());
		MovesBuckets.moveBucket(bucket, directoryToMoveTo);
		assertFalse(bucket.getDirectory().exists());
	}

	@Test(expectedExceptions = { FileNotDirectoryException.class })
	public void moveBucket_givenNonDirectory_throwFileNotDirectoryException() {
		File file = createFile();
		assertFalse(file.isDirectory());
		MovesBuckets.moveBucket(TUtilsBucket.createBucket(), file);
	}

	@Test(expectedExceptions = { DirectoryDidNotExistException.class })
	public void moveBucket_givenNonExistingFile_throwDirectoryDidNotExistException() {
		File nonExistingDir = createFilePath();
		assertFalse(nonExistingDir.exists());
		MovesBuckets.moveBucket(TUtilsBucket.createBucket(), nonExistingDir);
	}

	public void moveBucket_givenDirectoryWithContents_contentShouldBeMoved()
			throws IOException {
		String contentsFileName = "contents";
		File contents = TUtilsFile.createFileInParent(bucket.getDirectory(),
				contentsFileName);
		TUtilsFile.populateFileWithRandomContent(contents);
		List<String> contentLines = IOUtils.readLines(new FileReader(contents));

		Bucket movedBucket = MovesBuckets.moveBucket(bucket, directoryToMoveTo);

		File movedContents = new File(movedBucket.getDirectory().getAbsolutePath(),
				contentsFileName);
		assertTrue(movedContents.exists());
		assertTrue(!contents.exists());
		List<String> movedContentLines = IOUtils.readLines(new FileReader(
				movedContents));
		assertEquals(contentLines, movedContentLines);
	}

	public void moveBucket_givenBucket_keepsTheFormatOfTheBucket()
			throws IOException {
		Bucket movedBucket = MovesBuckets.moveBucket(bucket, directoryToMoveTo);
		assertEquals(bucket.getFormat(), movedBucket.getFormat());
	}

	@Test(groups = { "slow-unit" })
	public void moveBucket_givenRealCsvBucket_keepsTheCsvFormat() {
		Bucket csvBucket = TUtilsBucket.createRealCsvBucket();
		Bucket movedBucket = MovesBuckets.moveBucket(csvBucket, directoryToMoveTo);
		assertEquals(csvBucket.getFormat(), movedBucket.getFormat());
	}

	@Test(expectedExceptions = { RemoteBucketException.class })
	public void moveBucketToDir_initWithNonFileUri_throwsRemoteBucketException()
			throws FileNotFoundException, FileNotDirectoryException {
		Bucket remoteBucket = TUtilsBucket.createRemoteBucket();
		MovesBuckets.moveBucket(remoteBucket, directoryToMoveTo);
	}
}
