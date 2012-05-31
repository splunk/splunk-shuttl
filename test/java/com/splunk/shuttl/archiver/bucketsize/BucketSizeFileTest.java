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
package com.splunk.shuttl.archiver.bucketsize;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketSizeFileTest {

	private BucketSizeFile bucketSizeFile;

	@BeforeMethod
	public void setUp() {
		bucketSizeFile = new BucketSizeFile();
	}

	public void getFileWithBucketSize_givenBucket_returnsFileWithSizeOfBucket()
			throws IOException {
		Bucket bucket = TUtilsBucket.createRealBucket();
		File fileWithBucketSize = bucketSizeFile.getFileWithBucketSize(bucket);
		List<String> linesOfFile = FileUtils.readLines(fileWithBucketSize);
		assertEquals(1, linesOfFile.size());
		String firstLine = linesOfFile.get(0);
		assertEquals(bucket.getSize() + "", firstLine);
	}

	public void getFileWithBucketSize_givenBucket_fileNameContainsBucketNameForUniquness() {
		Bucket bucket = TUtilsBucket.createBucket();
		File fileWithBucketSize = bucketSizeFile.getFileWithBucketSize(bucket);
		assertTrue(fileWithBucketSize.getName().contains(bucket.getName()));
	}

	public void getFileWithBucketSize_givenBucket_fileNameContainsSizeLitteralForExternalUnderstandingOfTheFile() {
		Bucket bucket = TUtilsBucket.createBucket();
		File fileWithBucketSize = bucketSizeFile.getFileWithBucketSize(bucket);
		assertTrue(fileWithBucketSize.getName().contains("size"));
	}
}
