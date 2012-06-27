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
import java.net.URI;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFunctional;

@Test(groups = { "functional" })
public class BucketSizeIOFunctionalTest {

	private BucketSizeIO bucketSizeIO;
	private ArchiveFileSystem archiveFileSystem;
	private PathResolver pathResolver;
	private ArchiveConfiguration localConfig;

	@BeforeMethod
	public void setUp() {
		localConfig = TUtilsFunctional.getLocalFileSystemConfiguration();
		archiveFileSystem = ArchiveFileSystemFactory
				.getWithConfiguration(localConfig);
		pathResolver = new PathResolver(localConfig);
		bucketSizeIO = new BucketSizeIO(archiveFileSystem);
	}

	@AfterMethod
	public void tearDown() {
		TUtilsFunctional.tearDownLocalConfig(localConfig);
	}

	public void BucketSizeIO_givenHadoopFileSystem_putsFileWithSizeAndReadsIt()
			throws IOException {
		Bucket bucket = TUtilsBucket.createRealBucket();
		File fileWithBucketSize = bucketSizeIO.getFileWithBucketSize(bucket);
		long bucketSize = bucket.getSize();
		URI uriForFileWithBucketSize = pathResolver
				.getBucketSizeFileUriForBucket(bucket);

		archiveFileSystem.putFileAtomically(fileWithBucketSize,
				uriForFileWithBucketSize);

		long remotelyReadSize = bucketSizeIO
				.readSizeFromRemoteFile(uriForFileWithBucketSize);
		assertEquals(bucketSize, remotelyReadSize);
	}

}
