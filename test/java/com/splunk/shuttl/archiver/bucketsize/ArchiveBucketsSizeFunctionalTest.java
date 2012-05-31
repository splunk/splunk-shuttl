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

import java.util.List;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.listers.ListsBucketsFiltered;
import com.splunk.shuttl.archiver.listers.ListsBucketsFilteredFactory;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFunctional;

@Test(groups = { "functional" })
public class ArchiveBucketsSizeFunctionalTest {

	private ArchiveConfiguration config;
	private ArchiveBucketSize archiveBucketSize;
	private ListsBucketsFiltered listsBucketsFiltered;

	@BeforeMethod
	public void setUp() {
		config = TUtilsFunctional.getLocalFileSystemConfiguration();
		ArchiveFileSystem archiveFileSystem = ArchiveFileSystemFactory
				.getWithConfiguration(config);
		PathResolver pathResolver = new PathResolver(config);
		archiveBucketSize = ArchiveBucketSize.create(pathResolver,
				archiveFileSystem);
		listsBucketsFiltered = ListsBucketsFilteredFactory.create(config);
	}

	@AfterMethod
	public void tearDown() {
		TUtilsFunctional.tearDownLocalConfig(config);
	}

	public void _givenArchivedBucket_putSizeOfBucketThenListRemoteBucketAndGetSize() {
		Bucket realBucket = TUtilsBucket.createRealBucket();
		long realBucketSize = realBucket.getSize();
		TUtilsFunctional.archiveBucket(realBucket, config);

		List<Bucket> buckets = listsBucketsFiltered
				.listFilteredBucketsAtIndex(realBucket.getIndex(),
						realBucket.getEarliest(), realBucket.getLatest());
		assertEquals(1, buckets.size());
		Bucket remoteBucket = buckets.get(0);
		assertRealAndRemoteBucketsAreEqual(realBucket, remoteBucket);

		long remoteBucketSize = archiveBucketSize.getSize(remoteBucket);
		assertEquals(realBucketSize, remoteBucketSize);
	}

	private void assertRealAndRemoteBucketsAreEqual(Bucket realBucket,
			Bucket remoteBucket) {
		assertEquals(realBucket.getName(), remoteBucket.getName());
		assertEquals(realBucket.getIndex(), remoteBucket.getIndex());
	}
}
