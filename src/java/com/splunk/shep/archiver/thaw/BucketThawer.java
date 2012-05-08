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
package com.splunk.shep.archiver.thaw;

import static com.splunk.shep.archiver.LogFormatter.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.splunk.shep.archiver.listers.ArchiveBucketsLister;
import com.splunk.shep.archiver.model.Bucket;

/**
 * Interacts with the archive to thaw buckets within the users needs, which is
 * currently only a time range.
 */
public class BucketThawer {

    private static final Logger logger = Logger.getLogger(BucketThawer.class);
    private final ArchiveBucketsLister archiveBucketsLister;
    private final BucketFilter bucketFilter;
    private final BucketFormatResolver bucketFormatResolver;
    private final ThawBucketTransferer thawBucketTransferer;

    /**
     * @param bucketsLister
     *            used for listing buckets in the archive.
     * @param bucketFilter
     *            filtering buckets to only get the buckets that satisfies the
     *            thawing needs.
     * @param bucketFormatResolver
     *            to resolve the format to thaw for the bucket.
     * @param thawBucketTransferer
     *            for transferring the buckets to thawed.
     */
    public BucketThawer(ArchiveBucketsLister bucketsLister,
	    BucketFilter bucketFilter,
	    BucketFormatResolver bucketFormatResolver,
	    ThawBucketTransferer thawBucketTransferer) {
	this.archiveBucketsLister = bucketsLister;
	this.bucketFilter = bucketFilter;
	this.bucketFormatResolver = bucketFormatResolver;
	this.thawBucketTransferer = thawBucketTransferer;
    }

    /**
     * Thaw buckets by listing buckets in an index, filter the buckets, resolve
     * their formats and lastly transferring them to the thaw directory.
     */
    public Map<String, List<Bucket>> thawBuckets(String index,
	    Date earliestTime, Date latestTime) {
	List<Bucket> bucketsInIndex = archiveBucketsLister
		.listBucketsInIndex(index);
	List<Bucket> filteredBuckets = bucketFilter.filterBucketsByTimeRange(
		bucketsInIndex, earliestTime, latestTime);
	List<Bucket> bucketsWithFormats = bucketFormatResolver
		.resolveBucketsFormats(filteredBuckets);

	List<Bucket> failedBuckets = new ArrayList<Bucket>();
	List<Bucket> thawedBuckets = new ArrayList<Bucket>();

	for (Bucket bucket : bucketsWithFormats) {
	    logger.info(will("Attempting to thaw bucket", "bucket", bucket));
	    try {
		thawBucketTransferer.transferBucketToThaw(bucket);
		thawedBuckets.add(bucket);
		logger.info(done("Thawed bucket", "bucket", bucket));
	    } catch (IOException e) {
		logger.error(did("Tried to thaw bucket", e,
			"Place the bucket in thaw", "bucket", bucket,
			"exception", e));
		failedBuckets.add(bucket);
	    }
	}

	HashMap<String, List<Bucket>> ret = new HashMap<String, List<Bucket>>();
	ret.put("thawed", thawedBuckets);
	ret.put("failed", failedBuckets);

	return ret;
    }
}
