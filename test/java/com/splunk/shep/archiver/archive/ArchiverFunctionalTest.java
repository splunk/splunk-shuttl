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
package com.splunk.shep.archiver.archive;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.server.mbeans.rest.BucketArchiverRest;
import com.splunk.shep.testutil.UtilsBucket;
import com.splunk.shep.testutil.UtilsFile;

@Test(enabled = false, groups = { "slow" })
public class ArchiverFunctionalTest {

    public void Archiver_givenExistingBucket_archiveIt()
	    throws InterruptedException, IOException {
	// Setup
	File tempDirectory = UtilsFile.createTempDirectory();
	Bucket bucket = UtilsBucket
		.createBucketWithSplunkBucketFormatInDirectory(tempDirectory);
	File bucketDirectory = bucket.getDirectory();
	File archivedBucketDirectory = getArchivedBucketDirectory(bucket);

	try {
	    // Test
	    // BucketFreezer.main(bucketDirectory.getAbsolutePath());
	    new BucketArchiverRest().archiveBucket(bucketDirectory
		    .getAbsolutePath());

	    // Wait for it..
	    int timeInMillis = 1000;
	    System.out.println(ArchiverFunctionalTest.class.getSimpleName()
		    + "sleeping for: " + timeInMillis + "ms");
	    Thread.sleep(timeInMillis);

	    // Verify
	    assertTrue(!bucketDirectory.exists());
	    assertTrue(archivedBucketDirectory.exists());
	} finally {
	    FileUtils.deleteDirectory(tempDirectory);
	    FileUtils.deleteDirectory(archivedBucketDirectory.getParentFile()
		    .getParentFile().getParentFile().getParentFile());
	    // This deletion assumes and knows too much. When we remove
	    // BucketFreezer, this will be ok tho.
	    FileUtils.deleteDirectory(new File(
		    BucketFreezer.DEFAULT_SAFE_LOCATION));
	}
    }

    private File getArchivedBucketDirectory(Bucket bucket) {
	PathResolver pathResolver = new PathResolver(new ArchiveConfiguration());
	URI resolvedArchivePath = pathResolver.resolveArchivePath(bucket);
	File archivedBucket = new File(resolvedArchivePath);
	return archivedBucket;
    }

}
