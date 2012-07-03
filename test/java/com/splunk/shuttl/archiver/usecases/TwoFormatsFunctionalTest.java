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

import static java.util.Arrays.*;
import static org.testng.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketArchiver;
import com.splunk.shuttl.archiver.archive.BucketArchiverFactory;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFunctional;

@Test(groups = { "end-to-end" })
public class TwoFormatsFunctionalTest {

	private BucketArchiver bucketArchiver;
	private PathResolver pathResolver;
	private ArchiveFileSystem archiveFileSystem;

	@BeforeMethod
	public void setUp() {
		ArchiveConfiguration config = TUtilsFunctional
				.getLocalConfigurationThatArchivesFormats(asList(
						BucketFormat.SPLUNK_BUCKET, BucketFormat.CSV));
		archiveFileSystem = ArchiveFileSystemFactory.getWithConfiguration(config);
		pathResolver = new PathResolver(config);
		bucketArchiver = BucketArchiverFactory
				.createWithConfigurationAndArchiveFileSystem(config, archiveFileSystem);
	}

	@Parameters(value = { "splunk.home" })
	public void _givenTwoFormats_archivesBothFormats(String splunkHome)
			throws IOException {
		Bucket realBucket = TUtilsBucket.createRealBucket();
		TUtilsFunctional.archiveBucket(realBucket, bucketArchiver, splunkHome);

		assertBucketIsArchivedInFormat(realBucket, BucketFormat.SPLUNK_BUCKET);
		assertBucketIsArchivedInFormat(realBucket, BucketFormat.CSV);
		assertFalse(realBucket.getDirectory().exists());
	}

	private void assertBucketIsArchivedInFormat(Bucket realBucket,
			BucketFormat format) throws IOException {
		URI splunkBucketUri = pathResolver.resolveArchivedBucketURI(
				realBucket.getIndex(), realBucket.getName(), format);
		List<URI> listPath = archiveFileSystem.listPath(splunkBucketUri);
		assertFalse(listPath.contains(splunkBucketUri));
		assertFalse(listPath.isEmpty());
	}
}
