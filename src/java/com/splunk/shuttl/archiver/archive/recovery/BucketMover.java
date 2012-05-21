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
package com.splunk.shuttl.archiver.archive.recovery;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.FileNotDirectoryException;

/**
 * Class for moving buckets to the location passed to
 * {@link #BucketMover(String)}
 */
public class BucketMover {

    private final static Logger logger = Logger.getLogger(BucketMover.class);
    private final File movedBucketsLocation;

    /**
     * @param movedBucketsLocationPath
     *            path to the failed buckets location
     */
    public BucketMover(File movedBucketsLocation) {
	this.movedBucketsLocation = movedBucketsLocation;
    }

    /**
     * Move a bucket to the location passed to the constructor
     * {@link #BucketMover(String)}
     * 
     * @param bucket
     *            to move
     * @return the new bucket moved to the new location.
     */
    public Bucket moveBucket(Bucket bucket) {
	logger.debug(will("moving bucket", "bucket", bucket, "destination",
		movedBucketsLocation));

	Bucket movedBucket = null;
	try {
	    movedBucket = moveBucketToMovedBucketsLocationAndPerserveItsIndex(bucket);
	    logger.debug(did("moved bucket", "success", null, "bucket", bucket,
		    "destination", movedBucketsLocation));
	    return movedBucket;
	} catch (FileNotFoundException e) {
	    logger.error(did("Tried to move bucket",
		    "Destination did not exist", null, "bucket", bucket,
		    "exception", e, "destination", movedBucketsLocation));
	    throw new RuntimeException(e);
	} catch (FileNotDirectoryException e) {
	    logger.error(did("Tried to move bucket",
		    "Destination was not a directory", null, "bucket", bucket,
		    "exception", e, "destination", movedBucketsLocation));
	    throw new RuntimeException(e);
	}

    }

    private Bucket moveBucketToMovedBucketsLocationAndPerserveItsIndex(
	    Bucket bucket) throws FileNotFoundException,
	    FileNotDirectoryException {
	File indexDirectory = new File(movedBucketsLocation, bucket.getIndex());
	indexDirectory.mkdirs();
	return bucket.moveBucketToDir(indexDirectory);
    }

    /**
     * @return list of buckets in the failed buckets location that can be
     *         transfered
     */
    public List<Bucket> getMovedBuckets() {
	ArrayList<Bucket> movedBuckets = new ArrayList<Bucket>();

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
	    logger.debug(did("Created bucket from file",
		    "Got FileNotFoundException", "To create bucket from file",
		    "file", bucketFile, "exception", e));
	    throw new RuntimeException(e);
	} catch (FileNotDirectoryException e) {
	    logger.debug(did("Created bucket from file",
		    "Got FileNotDirectoryException",
		    "To create bucket from file", "file", bucketFile,
		    "exception", e));
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }
}
