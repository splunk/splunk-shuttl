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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.ShellClassRunner;
import com.splunk.shep.testutil.UtilsBucket;

@Test(enabled = false, groups = { "slow" })
public class ArchiverFunctionalTest {

    private FileSystem fileSystem;

    @BeforeMethod(groups = { "super-slow" })
    // @Parameters(value = { "hadoop.host", "hadoop.port" })
    // public void setUp(String hadoopHost, String hadoopPort) throws
    // IOException {
    public void setUp() throws IOException {
	String hadoopHost = "localhost";
	String hadoopPort = "9000"; // THIS IS NOT OK.!
	fileSystem = FileSystem.get(
		URI.create("hdfs://" + hadoopHost + ":" + hadoopPort),
		new Configuration());
    }

    public void Archiver_givenExistingBucket_archiveIt()
	    throws InterruptedException, IOException {
	Path archivedPath = null;

	// Setup
	try {
	    Bucket bucket = UtilsBucket.createTestBucket();
	    File bucketDirectory = bucket.getDirectory();
	    URI archivedUri = getArchivedBucketURI(bucket);
	    archivedPath = new Path(archivedUri);

	    // Test
	    ShellClassRunner shellClassRunner = new ShellClassRunner();
	    shellClassRunner.runClassWithArgs(BucketFreezer.class,
		    bucket.getIndex(), bucketDirectory.getAbsolutePath());
	    // new BucketArchiverRest.BucketArchiverRunner(
	    // bucketDirectory.getAbsolutePath());

	    // Wait for it..
	    int timeInMillis = 500;
	    System.out.println(ArchiverFunctionalTest.class.getSimpleName()
		    + " sleeping for: " + timeInMillis + "ms");
	    Thread.sleep(timeInMillis);

	    // Verify
	    assertTrue(!bucketDirectory.exists());
	    assertTrue(fileSystem.exists(archivedPath));
	} finally {
	    if (archivedPath != null) {
		fileSystem.delete(archivedPath.getParent().getParent()
			.getParent().getParent().getParent(), true);
	    }
	    // This deletion assumes and knows too much. When we remove
	    // BucketFreezer, this will be ok tho.
	    FileUtils.deleteDirectory(new File(
		    BucketFreezer.DEFAULT_SAFE_LOCATION));
	}
    }

    /**
     * @param bucket
     * @return
     */
    private URI getArchivedBucketURI(Bucket bucket) {
	PathResolver pathResolver = BucketArchiverFactory
		.createDefaultArchiver().getPathResolver();
	return pathResolver.resolveArchivePath(bucket);
    }
}
