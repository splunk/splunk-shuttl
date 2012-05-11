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
package com.splunk.shep.archiver.functional;

import static com.splunk.shep.testutil.UtilsFile.*;
import static java.util.Arrays.*;
import static org.testng.AssertJUnit.*;

import java.net.URI;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.ArchiveConfiguration;
import com.splunk.shep.archiver.archive.BucketArchiver;
import com.splunk.shep.archiver.archive.BucketArchiverFactory;
import com.splunk.shep.archiver.archive.BucketFormat;
import com.splunk.shep.archiver.archive.PathResolver;
import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.fileSystem.ArchiveFileSystemFactory;
import com.splunk.shep.archiver.listers.ArchiveBucketsLister;
import com.splunk.shep.archiver.listers.ArchivedIndexesLister;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.thaw.BucketFormatChooser;
import com.splunk.shep.archiver.thaw.BucketFormatResolver;
import com.splunk.shep.testutil.UtilsBucket;
import com.splunk.shep.testutil.UtilsEnvironment;

@Test(enabled = true, groups = { "integration" })
public class ExportCsvFunctionalTest {

    private BucketArchiver csvBucketArchiver;
    private ArchiveBucketsLister bucketsLister;
    private BucketFormatResolver bucketFormatResolver;

    @BeforeMethod
    public void setUp() {
	ArchiveConfiguration csvConfig = constructCsvArchiveConfigration();
	ArchiveFileSystem localFileSystem = ArchiveFileSystemFactory
		.getWithConfiguration(csvConfig);
	csvBucketArchiver = BucketArchiverFactory
		.createWithConfigurationAndArchiveFileSystem(csvConfig,
			localFileSystem);
	PathResolver pathResolver = new PathResolver(csvConfig);
	ArchivedIndexesLister indexesLister = new ArchivedIndexesLister(
		pathResolver, localFileSystem);
	bucketsLister = new ArchiveBucketsLister(localFileSystem,
		indexesLister, pathResolver);
	BucketFormatChooser bucketFormatChooser = new BucketFormatChooser(
		csvConfig);
	bucketFormatResolver = new BucketFormatResolver(pathResolver,
		localFileSystem, bucketFormatChooser);
    }

    private ArchiveConfiguration constructCsvArchiveConfigration() {
	String archivePath = createTempDirectory().getAbsolutePath();
	URI archivingRoot = URI.create("file:" + archivePath);
	URI tmpDirectory = URI.create("file:/tmp");
	BucketFormat bucketFormat = BucketFormat.CSV;
	ArchiveConfiguration config = new ArchiveConfiguration(bucketFormat,
		archivingRoot, "clusterName", "serverName",
		asList(bucketFormat), tmpDirectory);
	return config;
    }

    @Parameters(value = { "splunk.home" })
    public void _givenSplunkHomeAndBucketInSplunkBucketFormat_archivedAsCsvFormat(
	    final String splunkHome) {
	UtilsEnvironment.runInCleanEnvironment(new Runnable() {

	    @Override
	    public void run() {
		UtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME",
			splunkHome);
		archiveBucketInSplunkBucketFormatAsCsv();

	    }
	});
    }

    private void archiveBucketInSplunkBucketFormatAsCsv() {
	Bucket bucket = UtilsBucket.copyRealBucket();
	assertEquals(BucketFormat.SPLUNK_BUCKET, bucket.getFormat());
	csvBucketArchiver.archiveBucket(bucket);
	List<Bucket> buckets = bucketsLister.listBucketsInIndex(bucket
		.getIndex());
	List<Bucket> bucketsWithFormats = bucketFormatResolver
		.resolveBucketsFormats(buckets);

	assertEquals(1, bucketsWithFormats.size());
	assertEquals(BucketFormat.CSV, bucketsWithFormats.get(0).getFormat());
    }
}
