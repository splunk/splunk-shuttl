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
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.LocalFileSystemConstants;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketArchiver;
import com.splunk.shuttl.archiver.archive.BucketArchiverFactory;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.listers.ArchiveBucketsLister;
import com.splunk.shuttl.archiver.listers.ArchivedIndexesLister;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.thaw.BucketFormatChooser;
import com.splunk.shuttl.archiver.thaw.BucketFormatResolver;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFunctional;

@Test(groups = { "end-to-end" })
public class ExportCsvFunctionalTest {

	private BucketArchiver csvBucketArchiver;
	private ArchiveBucketsLister bucketsLister;
	private BucketFormatResolver bucketFormatResolver;
	private Bucket bucket;
	private File archiverData;

	@BeforeMethod
	public void setUp() {
		archiverData = createDirectory();
		ArchiveConfiguration csvConfig = TUtilsFunctional
				.getLocalCsvArchiveConfigration();
		ArchiveFileSystem localFileSystem = ArchiveFileSystemFactory
				.getWithConfiguration(csvConfig);
		csvBucketArchiver = BucketArchiverFactory
				.createWithConfFileSystemAndCsvDirectory(csvConfig, localFileSystem,
						new LocalFileSystemConstants(archiverData.getAbsolutePath()));
		PathResolver pathResolver = new PathResolver(csvConfig);
		ArchivedIndexesLister indexesLister = new ArchivedIndexesLister(
				pathResolver, localFileSystem);
		bucketsLister = new ArchiveBucketsLister(localFileSystem, indexesLister,
				pathResolver);
		BucketFormatChooser bucketFormatChooser = new BucketFormatChooser(csvConfig);
		bucketFormatResolver = new BucketFormatResolver(pathResolver,
				localFileSystem, bucketFormatChooser);

		bucket = TUtilsBucket.createRealBucket();
	}

	@AfterMethod
	public void tearDown() {
		FileUtils.deleteQuietly(bucket.getDirectory());
		FileUtils.deleteQuietly(archiverData);
	}

	@Parameters(value = { "splunk.home" })
	public void archiveBucketAsCsv_givenSplunkHomeAndBucketInSplunkBucketFormat_archivedAsCsvFormat(
			final String splunkHome) {
		TUtilsFunctional.archiveBucket(bucket, csvBucketArchiver, splunkHome);
		verifyBucketWasArchivedAsCsv();
	}

	private void verifyBucketWasArchivedAsCsv() {
		List<Bucket> buckets = bucketsLister.listBucketsInIndex(bucket.getIndex());
		List<Bucket> bucketsWithFormats = bucketFormatResolver
				.resolveBucketsFormats(buckets);

		assertEquals(1, bucketsWithFormats.size());
		assertEquals(BucketFormat.CSV, bucketsWithFormats.get(0).getFormat());
	}

}
