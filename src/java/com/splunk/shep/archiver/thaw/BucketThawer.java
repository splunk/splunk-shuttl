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

import java.util.Date;
import java.util.List;

import com.splunk.shep.archiver.listers.ArchiveBucketsLister;
import com.splunk.shep.archiver.model.Bucket;

/**
 * Interacts with the archive to thaw buckets within the users needs, which is
 * currently only a time range.
 */
public class BucketThawer {

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
    public void thawBuckets(String index, Date earliestTime, Date latestTime) {
	List<Bucket> buckets = archiveBucketsLister.listBucketsInIndex(index);
	List<Bucket> filteredBuckets = bucketFilter.filterBucketsByTimeRange(
		buckets, earliestTime, latestTime);
	List<Bucket> bucketsWithFormats = bucketFormatResolver
		.resolveBucketsFormats(filteredBuckets);
	for (Bucket bucket : bucketsWithFormats) {
	    thawBucketTransferer.transferBucketToThaw(bucket);
	}
    }
}
