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
package com.splunk.shuttl.archiver.copy;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsTestNG;

@Test(groups = { "fast-unit" })
public class BucketIteratorFactoryTest {

	private BucketIteratorFactory iteratorFactory;
	private String index = "index";

	@BeforeMethod
	public void setUp() {
		iteratorFactory = new BucketIteratorFactory();

	}

	private List<LocalBucket> getIteratedBuckets(File directory) {
		List<LocalBucket> iteratedBuckets = new ArrayList<LocalBucket>();
		for (LocalBucket b : iteratorFactory.iteratorInDirectory(directory, index))
			iteratedBuckets.add(b);
		return iteratedBuckets;
	}

	public void _emptyDirectory_noBucketsToIterateOver() {
		File dir = createDirectory();
		assertIteratesOverNoBuckets(dir);
	}

	private void assertIteratesOverNoBuckets(File dir) {
		Iterable<LocalBucket> iterable = iteratorFactory.iteratorInDirectory(dir,
				index);
		for (LocalBucket b : iterable)
			fail("Should have nothing to iterate over. Found: " + b);
	}

	public void _directoryWithFile_noBucketsToIterateOver() {
		File dir = createDirectory();
		createFileInParent(dir, "just-random.file");

		assertIteratesOverNoBuckets(dir);
	}

	public void _directoryWithBucketAndDirectoryThatDoesNotHaveBucketName_iteratesOverBucket() {
		File dir = createDirectory();
		createDirectoryInParent(dir, "not-a-bucket-name");

		assertIteratesOverNoBuckets(dir);
	}

	public void _directoryWithBucket_iteratesOverBucket() {
		File directory = createDirectory();
		LocalBucket bucketInDirectory = TUtilsBucket
				.createBucketInDirectory(directory);

		assertIteratesOverBucketInDir(bucketInDirectory, directory);
	}

	private void assertIteratesOverBucketInDir(LocalBucket bucketInDirectory,
			File directory) {
		List<LocalBucket> iteratedBuckets = getIteratedBuckets(directory);
		assertEquals(1, iteratedBuckets.size());
		TUtilsTestNG.isBucketEqualOnIndexFormatAndName(iteratedBuckets.get(0),
				bucketInDirectory);
	}

	public void _directoryWithMixedFilesAndOneBucket_iteratesOverBucket() {
		File dir = createDirectory();
		createFileInParent(dir, "some.file");
		createDirectoryInParent(dir, "some-dir");
		LocalBucket bucketInDirectory = TUtilsBucket.createBucketInDirectory(dir);

		assertIteratesOverBucketInDir(bucketInDirectory, dir);
	}
}
