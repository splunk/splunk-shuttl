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

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketArchiver;
import com.splunk.shuttl.archiver.archive.BucketArchiverFactory;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.listers.ListsBucketsFiltered;
import com.splunk.shuttl.archiver.listers.ListsBucketsFilteredFactory;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFunctional;

@Test(groups = { "functional" })
public class ArchiveBucketsSizeFunctionalTest {

	private ArchiveConfiguration config;
	private ArchiveBucketSize archiveBucketSize;
	private ListsBucketsFiltered listsBucketsFiltered;
	private File archiverData;
	private BucketArchiver bucketArchiver;

	@BeforeMethod
	public void setUp() {
		config = TUtilsFunctional.getLocalFileSystemConfiguration();
		ArchiveFileSystem archiveFileSystem = ArchiveFileSystemFactory
				.getWithConfiguration(config);
		archiverData = createDirectory();
		LocalFileSystemPaths localFileSystemPaths = new LocalFileSystemPaths(
				archiverData.getAbsolutePath());
		PathResolver pathResolver = new PathResolver(config);
		archiveBucketSize = ArchiveBucketSize.create(pathResolver,
				archiveFileSystem, localFileSystemPaths);
		listsBucketsFiltered = ListsBucketsFilteredFactory.create(config);
		bucketArchiver = BucketArchiverFactory
				.createWithConfFileSystemAndCsvDirectory(config, archiveFileSystem,
						localFileSystemPaths);
	}

	@AfterMethod
	public void tearDown() {
		TUtilsFunctional.tearDownLocalConfig(config);
		FileUtils.deleteQuietly(archiverData);
	}

	public void _givenArchivedBucket_putSizeOfBucketThenListRemoteBucketAndGetSize() {
		LocalBucket realBucket = TUtilsBucket.createRealBucket();
		long realBucketSize = realBucket.getSize();
		TUtilsFunctional.archiveBucket(realBucket, bucketArchiver);

		List<Bucket> buckets = listsBucketsFiltered
				.listFilteredBucketsAtIndex(realBucket.getIndex(),
						realBucket.getEarliest(), realBucket.getLatest());
		assertEquals(1, buckets.size());
		Bucket remoteBucket = buckets.get(0);
		assertRealAndRemoteBucketsAreEqual(realBucket, remoteBucket);

		long remoteBucketSize = archiveBucketSize.readBucketSize(remoteBucket);
		assertEquals(realBucketSize, remoteBucketSize);
	}

	private void assertRealAndRemoteBucketsAreEqual(Bucket realBucket,
			Bucket remoteBucket) {
		assertEquals(realBucket.getName(), remoteBucket.getName());
		assertEquals(realBucket.getIndex(), remoteBucket.getIndex());
	}
}
