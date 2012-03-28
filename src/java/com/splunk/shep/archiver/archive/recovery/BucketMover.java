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
 * Class for moving buckets to the location passed to
 * {@link #BucketMover(String)}
 */
public class BucketMover {

    private final String movedBucketsLocationPath;

    /**
     * @param movedBucketsLocationPath
     *            path to the failed buckets location
     */
    public BucketMover(String movedBucketsLocationPath) {
	this.movedBucketsLocationPath = movedBucketsLocationPath;
    }

    /**
     * Move a bucket to the location passed to the constructor
     * {@link #BucketMover(String)}
     * 
     * @param bucket
     *            to move
     * @return the new bucket moved to the new location.
     */
    public Bucket moveBucket(Bucket movedBucket) {
	try {
	    return moveBucketToMovedBucketsLocationAndPerserveItsIndex(movedBucket);
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	} catch (FileNotDirectoryException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    private Bucket moveBucketToMovedBucketsLocationAndPerserveItsIndex(
	    Bucket bucket) throws FileNotFoundException,
	    FileNotDirectoryException {
	File indexDirectory = new File(getMovedBucketsLocation(),
		bucket.getIndex());
	indexDirectory.mkdirs();
	return bucket.moveBucketToDir(indexDirectory);
    }

    /**
     * @return list of buckets in the failed buckets location that can be
     *         transfered
     */
    public List<Bucket> getMovedBuckets() {
	ArrayList<Bucket> movedBuckets = new ArrayList<Bucket>();

	File movedBucketsLocation = getMovedBucketsLocation();
	File[] listFiles = movedBucketsLocation.listFiles();
	// This will fail when the buckets structure has changed.
	if (listFiles != null) {
	    for (File file : listFiles) {
		if (file.isFile()) {
		    continue; // Ignore regular files.
		} else {
		    addBucketsFromIndexDirectory(movedBuckets, file);
		}
	    }
	}
	return movedBuckets;
    }

    private void addBucketsFromIndexDirectory(ArrayList<Bucket> movedBuckets,
	    File file) {
	String index = file.getName();
	File[] bucketsInIndex = file.listFiles();
	if (bucketsInIndex != null) {
	    for (File bucket : bucketsInIndex) {
		movedBuckets.add(createBucketWithErrorHandling(index, bucket));
	    }
	}
    }

    private Bucket createBucketWithErrorHandling(String index, File bucketFile) {
	try {
	    return new Bucket(index, bucketFile);
	} catch (FileNotFoundException e) {
	    did("Created bucket from file", "Got FileNotFoundException",
		    "To create bucket from file", "file", bucketFile,
		    "exception", e);
	    throw new RuntimeException(e);
	} catch (FileNotDirectoryException e) {
	    did("Created bucket from file", "Got FileNotDirectoryException",
		    "To create bucket from file", "file", bucketFile,
		    "exception", e);
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    private File getMovedBucketsLocation() {
	return new File(movedBucketsLocationPath);
    }

}
