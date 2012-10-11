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
import static java.util.Arrays.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketArchiver;
import com.splunk.shuttl.archiver.archive.BucketArchiverFactory;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFunctional;

@Test(groups = { "end-to-end" })
public class TwoFormatsFunctionalTest {

	private BucketArchiver bucketArchiver;
	private PathResolver pathResolver;
	private ArchiveFileSystem archiveFileSystem;
	private File archiverData;

	@BeforeMethod
	public void setUp() {
		ArchiveConfiguration config = TUtilsFunctional
				.getLocalConfigurationThatArchivesFormats(asList(
						BucketFormat.SPLUNK_BUCKET, BucketFormat.CSV));
		archiveFileSystem = ArchiveFileSystemFactory.getWithConfiguration(config);
		archiverData = createDirectory();
		bucketArchiver = BucketArchiverFactory
				.createWithConfFileSystemAndCsvDirectory(config, archiveFileSystem,
						new LocalFileSystemPaths(archiverData.getAbsolutePath()));
		pathResolver = new PathResolver(config);
	}

	@AfterMethod
	public void tearDown() {
		FileUtils.deleteQuietly(archiverData);
	}

	@Parameters(value = { "splunk.home" })
	public void _givenTwoFormats_archivesBothFormats(String splunkHome)
			throws IOException {
		LocalBucket realBucket = TUtilsBucket.createRealBucket();
		TUtilsFunctional.archiveBucket(realBucket, bucketArchiver, splunkHome);

		assertBucketIsArchivedInFormat(realBucket, BucketFormat.SPLUNK_BUCKET);
		assertBucketIsArchivedInFormat(realBucket, BucketFormat.CSV);
		assertFalse(realBucket.getDirectory().exists());
	}

	private void assertBucketIsArchivedInFormat(Bucket realBucket,
			BucketFormat format) throws IOException {
		String splunkBucketPath = pathResolver.resolveArchivedBucketURI(
				realBucket.getIndex(), realBucket.getName(), format);
		List<String> listPath = archiveFileSystem.listPath(splunkBucketPath);
		assertFalse(listPath.contains(splunkBucketPath));
		assertFalse(listPath.isEmpty());
	}
}
