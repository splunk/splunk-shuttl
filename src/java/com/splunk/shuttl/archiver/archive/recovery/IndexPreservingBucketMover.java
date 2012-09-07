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
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.archiver.model.FileNotDirectoryException;
import com.splunk.shuttl.archiver.model.MovesBuckets;

/**
 * Preserves a bucket's index while moving it, by moving the bucket to a
 * directory that has the bucket's index name. <br/>
 * <br/>
 * Note: This class is used right when the buckets are being archived, before
 * any format changes are being made to the buckets. This means that the class
 * will only handle {@link BucketFormat#SPLUNK_BUCKET}. If the class want's to
 * be re-used with other formats, then the formats would need to be preserved
 * with a directory structure, like the indexes are preserved.
 */
public class IndexPreservingBucketMover {

	private final static Logger logger = Logger
			.getLogger(IndexPreservingBucketMover.class);

	/**
	 * Read the class comment to understand what this means.
	 * 
	 * @see IndexPreservingBucketMover
	 */
	private static final BucketFormat ONLY_VALID_BUCKET_FORMAT = BucketFormat.SPLUNK_BUCKET;

	private final File movedBucketsLocation;

	/**
	 * @param movedBucketsLocationPath
	 *          path to the failed buckets location
	 */
	private IndexPreservingBucketMover(File movedBucketsLocation) {
		this.movedBucketsLocation = movedBucketsLocation;
	}

	/**
	 * Move a bucket to the location passed to the constructor
	 * {@link #BucketMover(String)}
	 * 
	 * @param bucket
	 *          to move
	 * @return the new bucket moved to the new location.
	 */
	public Bucket moveBucket(Bucket bucket) {
		logger.debug(will("moving bucket", "bucket", bucket, "destination",
				movedBucketsLocation));
		Bucket movedBucket = moveBucketToMovedBucketsLocationAndPerserveItsIndex(bucket);
		logger.debug(did("moved bucket", "success", null, "bucket", bucket,
				"destination", movedBucketsLocation));
		return movedBucket;
	}

	private Bucket moveBucketToMovedBucketsLocationAndPerserveItsIndex(
			Bucket bucket) {
		File indexDirectory = new File(movedBucketsLocation, bucket.getIndex());
		indexDirectory.mkdirs();
		return MovesBuckets.moveBucket(bucket, indexDirectory);
	}

	/**
	 * @return list of buckets in the failed buckets location that can be
	 *         transfered
	 */
	public List<Bucket> getMovedBuckets() {
		ArrayList<Bucket> movedBuckets = new ArrayList<Bucket>();

		File[] listFiles = movedBucketsLocation.listFiles();
		if (listFiles != null)
			for (File file : listFiles)
				if (!file.isFile())
					addBucketsFromIndexDirectory(movedBuckets, file);
				else
					continue; // Ignore regular files.
		return movedBuckets;
	}

	private void addBucketsFromIndexDirectory(ArrayList<Bucket> movedBuckets,
			File file) {
		String index = file.getName();
		File[] bucketsInIndex = file.listFiles();
		if (bucketsInIndex != null)
			for (File bucket : bucketsInIndex)
				movedBuckets.add(BucketFactory.createBucketWithIndexDirectoryAndFormat(
						index, bucket, ONLY_VALID_BUCKET_FORMAT));
	}

	/**
	 * @param moveLocationDirectory
	 *          where the buckets will be moved to.
	 * @return instance of a BucketMover.
	 * @throws FileNotDirectoryException
	 *           when the file is not a directory.
	 * @throws DirectoryNotCreatableException
	 *           if the file doesn't exist and the file cannot be created as a
	 *           directory.
	 */
	public static IndexPreservingBucketMover create(File moveLocationDirectory) {
		verifyMoveLocationRequirements(moveLocationDirectory);
		return new IndexPreservingBucketMover(moveLocationDirectory);
	}

	private static void verifyMoveLocationRequirements(File file) {
		if (file.exists())
			verifyThatFileIsADirectory(file);
		else
			verifyThatFileCanBeCreatedAsADirectory(file);
	}

	private static void verifyThatFileIsADirectory(File file) {
		if (!file.isDirectory())
			throw new FileNotDirectoryException(
					"BucketMover's move location needs to be a directory. Was file: "
							+ file);
	}

	private static void verifyThatFileCanBeCreatedAsADirectory(File file) {
		if (!file.mkdirs())
			throw new DirectoryNotCreatableException(
					"Could not create BucketMover's move location: " + file);
	}

}
