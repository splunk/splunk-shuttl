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
package com.splunk.shep.archiver.archive.recovery;

import static com.splunk.shep.archiver.ArchiverLogger.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.model.FileNotDirectoryException;

/**
 * Class for moving and getting buckets that failed to be archived.<br/>
 * <br/>
 * Use {@link FailedBucketTransfers#moveFailedBucket(Bucket)} whenever a bucket
 * failed to be archived. Use {@link FailedBucketTransfers#getFailedBuckets()}
 * when it's time to do something about the failed buckets.
 * 
 */
public class FailedBucketTransfers {

    private final String failedBucketsLocationPath;

    /**
     * @param failedBucketsLocationPath
     *            path to the failed buckets location
     */
    public FailedBucketTransfers(String failedBucketsLocationPath) {
	this.failedBucketsLocationPath = failedBucketsLocationPath;
    }

    /**
     * Move a bucket which failed to be archived, to a location where it can
     * later be picked up and transfered again.
     * 
     * @param bucket
     *            that failed to archive.
     */
    public void moveFailedBucket(Bucket failedBucket) {
	try {
	    moveBucketToFailedBucketsLocationAndPerserveItsIndex(failedBucket);
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	} catch (FileNotDirectoryException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    private void moveBucketToFailedBucketsLocationAndPerserveItsIndex(
	    Bucket bucket) throws FileNotFoundException,
	    FileNotDirectoryException {
	File indexDirectory = new File(getFailedBucketsLocation(),
		bucket.getIndex());
	indexDirectory.mkdirs();
	bucket.moveBucketToDir(indexDirectory);
    }

    /**
     * @return list of buckets in the failed buckets location that can be
     *         transfered
     */
    public List<Bucket> getFailedBuckets() {
	ArrayList<Bucket> failedBuckets = new ArrayList<Bucket>();

	File failedBucketsLocation = getFailedBucketsLocation();
	File[] listFiles = failedBucketsLocation.listFiles();
	// This will fail when the buckets structure has changed.
	if (listFiles != null) {
	    for (File file : listFiles) {
		if (file.isFile()) {
		    continue; // Ignore regular files.
		} else {
		    addFailedBucket(failedBuckets, file);
		}
	    }
	}
	return failedBuckets;
    }

    private void addFailedBucket(ArrayList<Bucket> failedBuckets, File file) {
	Bucket bucket = getBucketFromFailedBucketsLocation(file);
	failedBuckets.add(bucket);
    }

    private Bucket getBucketFromFailedBucketsLocation(File file) {
	String index = file.getName();
	File bucketFile = file.listFiles()[0];
	try {
	    return new Bucket(index, bucketFile);
	} catch (FileNotFoundException e) {
	    did("Created bucket from file", "Got FileNotFoundException",
		    "To create bucket from file", "file", file, "exception", e);
	    throw new RuntimeException(e);
	} catch (FileNotDirectoryException e) {
	    did("Created bucket from file", "Got FileNotDirectoryException",
		    "To create bucket from file", "file", file, "exception", e);
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    private File getFailedBucketsLocation() {
	return new File(failedBucketsLocationPath);
    }

}
