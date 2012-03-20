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

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.BucketFreezer;
import com.splunk.shep.archiver.archive.recovery.BucketLock;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.ShellClassRunner;
import com.splunk.shep.testutil.UtilsBucket;

@Test(enabled = false, groups = { "super-slow" })
public class ArchiverFunctionalTest {

    private FileSystem fileSystem;

    @BeforeMethod
    // @Parameters(value = { "hadoop.host", "hadoop.port" })
    // public void setUp(String hadoopHost, String hadoopPort) throws
    // IOException {
    public void setUp() throws IOException {
	fileSystem = UtilsArchiverFunctional.getHadoopFileSystem();
    }

    @AfterMethod
    public void tearDown() throws IOException {
	FileUtils
		.deleteDirectory(new File(BucketFreezer.DEFAULT_SAFE_LOCATION));
	FileUtils.deleteDirectory(new File(BucketLock.DEFAULT_LOCKS_DIRECTORY));
    }

    public void Archiver_givenExistingBucket_archiveIt()
	    throws InterruptedException, IOException {
	// Setup
	try {
	    Bucket bucket = UtilsBucket.createTestBucket();
	    File bucketDirectory = bucket.getDirectory();

	    // Test
	    ShellClassRunner shellClassRunner = new ShellClassRunner();
	    shellClassRunner.runClassWithArgs(BucketFreezer.class,
		    bucket.getIndex(), bucketDirectory.getAbsolutePath());
	    // new BucketArchiverRest.BucketArchiverRunner(
	    // bucketDirectory.getAbsolutePath());

	    UtilsArchiverFunctional.waitForAsyncArchiving();

	    // Verify
	    URI archivedUri = UtilsArchiverFunctional
		    .getHadoopArchivedBucketURI(bucket);
	    assertTrue(!bucketDirectory.exists());
	    assertTrue(fileSystem.exists(new Path(archivedUri)));
	} finally {
	    UtilsArchiverFunctional.cleanArchivePathInHadoopFileSystem();
	}
    }

}
