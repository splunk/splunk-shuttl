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
package com.splunk.shuttl.archiver.usecases;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static com.splunk.shuttl.testutil.TUtilsFunctional.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
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
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "functional" })
public class ArchiverFunctionalTest {

	private ArchiveConfiguration config;
	private BucketArchiver bucketArchiver;
	private ArchiveFileSystem archiveFileSystem;
	private PathResolver pathResolver;
	private File archiverData;

	@BeforeMethod(groups = { "functional" })
	public void setUp() throws IOException {
		archiverData = createDirectory();
		config = getLocalFileSystemConfiguration();
		archiveFileSystem = ArchiveFileSystemFactory.getWithConfiguration(config);
		bucketArchiver = BucketArchiverFactory
				.createWithConfFileSystemAndCsvDirectory(config, archiveFileSystem,
						new LocalFileSystemPaths(archiverData.getAbsolutePath()));
		pathResolver = new PathResolver(config);
	}

	@AfterMethod
	public void tearDown() throws IOException {
		tearDownLocalConfig(config);
		FileUtils.deleteQuietly(archiverData);
	}

	public void Archiver_givenExistingBucket_archiveIt() throws IOException {
		Bucket bucket = TUtilsBucket.createBucket();
		int filesInBucket = bucket.getDirectory().listFiles().length;

		bucketArchiver.archiveBucket(bucket);

		URI bucketArchiveUri = pathResolver.resolveArchivePath(bucket);
		List<URI> urisInBucketDirectoryInArchive = archiveFileSystem
				.listPath(bucketArchiveUri);
		assertTrue(filesInBucket <= urisInBucketDirectoryInArchive.size());
	}

}
