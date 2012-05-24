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
package com.splunk.shuttl.archiver.functional;

import static com.splunk.shuttl.archiver.functional.UtilsFunctional.*;
import static org.testng.AssertJUnit.*;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketArchiver;
import com.splunk.shuttl.archiver.archive.BucketArchiverFactory;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.UtilsBucket;

@Test(groups = { "functional" })
public class ArchiverFunctionalTest {

    private ArchiveConfiguration config;
    private BucketArchiver bucketArchiver;
    private ArchiveFileSystem archiveFileSystem;

    @BeforeMethod(groups = { "functional" })
    public void setUp() throws IOException {
	config = getLocalFileSystemConfiguration();
	archiveFileSystem = ArchiveFileSystemFactory
		.getWithConfiguration(config);
	bucketArchiver = BucketArchiverFactory
		.createWithConfigurationAndArchiveFileSystem(config,
			archiveFileSystem);
    }

    @AfterMethod
    public void tearDown() throws IOException {
	tearDownLocalConfig(config);
    }

    public void Archiver_givenExistingBucket_archiveIt() throws IOException {
	Bucket bucket = UtilsBucket.createTestBucket();

	bucketArchiver.archiveBucket(bucket);

	verifyByListingBucketInArchiveFileSystem(bucket);
    }

    private void verifyByListingBucketInArchiveFileSystem(Bucket bucket)
	    throws IOException {
	URI bucketArchiveUri = bucketArchiver.getPathResolver()
		.resolveArchivePath(bucket);
	URI bucketParentUri = new Path(bucketArchiveUri).getParent().toUri();
	List<URI> urisAtBucketsParent = archiveFileSystem
		.listPath(bucketParentUri);
	assertEquals(1, urisAtBucketsParent.size());
	assertEquals(bucketArchiveUri, urisAtBucketsParent.get(0));
    }

}
