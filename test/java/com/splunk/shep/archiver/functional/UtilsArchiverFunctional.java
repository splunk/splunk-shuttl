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

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.splunk.shep.archiver.archive.BucketArchiverFactory;
import com.splunk.shep.archiver.archive.PathResolver;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;
import com.splunk.shep.testutil.UtilsTestNG;

/**
 * Util methods for functional archiver tests
 */
public class UtilsArchiverFunctional {
    /**
     * Uses the {@link BucketArchiverFactory#createDefaultArchiver()} to get its
     * {@link PathResolver} and retrieve the URI for the bucket as param.
     * 
     * @return URI to archived bucket in hadoop.
     */
    public static URI getHadoopArchivedBucketURI(Bucket bucket) {
	return getRealPathResolver().resolveArchivePath(bucket);
    }

    /**
     * @return path resolver used with the default archiver.
     */
    public static PathResolver getRealPathResolver() {
	return BucketArchiverFactory.createDefaultArchiver().getPathResolver();
    }

    /**
     * @return Hadoop {@link FileSystem} configured with shep's default host and
     *         port values.
     */
    public static FileSystem getHadoopFileSystem() {
	String hadoopHost = "localhost";
	String hadoopPort = "9000"; // TODO THIS IS NOT OK.!
	try {
	    return FileSystem.get(
		    URI.create("hdfs://" + hadoopHost + ":" + hadoopPort),
		    new Configuration());
	} catch (IOException e) {
	    e.printStackTrace();
	    UtilsTestNG.failForException("Couldn't get Hadoop file system.", e);
	    return null;
	}
    }

    public static void cleanArchivePathInHadoopFileSystem() {
	FileSystem hadoopFileSystem = getHadoopFileSystem();
	Bucket dummyBucket = UtilsBucket.createTestBucket();
	URI hadoopArchivedBucketURI = getHadoopArchivedBucketURI(dummyBucket);
	try {
	    hadoopFileSystem.delete(new Path(hadoopArchivedBucketURI)
		    .getParent().getParent().getParent().getParent()
		    .getParent(), true);
	} catch (IOException e) {
	    e.printStackTrace();
	    UtilsTestNG.failForException("Could not clean hadoop file system",
		    e);
	}
    }

    /**
     * 
     */
    public static void waitForAsyncArchiving() {
	try {
	    Thread.sleep(500);
	} catch (InterruptedException e) {
	    e.printStackTrace();
	    UtilsTestNG.failForException(
		    "Got interrupted when waiting for async archiving.", e);
	}
    }
}
