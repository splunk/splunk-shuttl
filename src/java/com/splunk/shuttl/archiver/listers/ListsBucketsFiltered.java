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
package com.splunk.shuttl.archiver.listers;

import java.util.Date;
import java.util.List;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.thaw.BucketFilter;
import com.splunk.shuttl.archiver.thaw.BucketFormatResolver;

/**
 * Lists buckets in the archive, filtered by time range.
 */
public class ListsBucketsFiltered {

	private final ArchiveBucketsLister bucketsLister;
	private final BucketFilter bucketFilter;
	private final BucketFormatResolver bucketFormatResolver;

	/**
	 * @param bucketsLister
	 * @param bucketFilter
	 * @param bucketFormatResolver
	 */
	public ListsBucketsFiltered(ArchiveBucketsLister bucketsLister,
			BucketFilter bucketFilter, BucketFormatResolver bucketFormatResolver) {
		this.bucketsLister = bucketsLister;
		this.bucketFilter = bucketFilter;
		this.bucketFormatResolver = bucketFormatResolver;
	}

	/**
	 * @return all archived buckets filtered by earliest and latest time.
	 * 
	 * @see ListsBucketsFiltered#listFilteredBucketsAtIndex(String, Date, Date)
	 */
	public List<Bucket> listFilteredBuckets(Date earliestTime, Date latestTime) {
		List<Bucket> allBuckets = bucketsLister.listBuckets();
		return filterBuckets(allBuckets, earliestTime, latestTime);
	}

	/**
	 * @return buckets that are archived in the specified index, filtered by
	 *         earliest and latest time.
	 * 
	 * @see ListsBucketsFiltered#listFilteredBuckets(Date, Date)
	 */
	public List<Bucket> listFilteredBucketsAtIndex(String index,
			Date earliestTime, Date latestTime) {
		List<Bucket> bucketsInIndex = bucketsLister.listBucketsInIndex(index);
		return filterBuckets(bucketsInIndex, earliestTime, latestTime);
	}

	private List<Bucket> filterBuckets(List<Bucket> bucketsToFilter,
			Date earliestTime, Date latestTime) {
		List<Bucket> filteredBuckets = bucketFilter.filterBucketsByTimeRange(
				bucketsToFilter, earliestTime, latestTime);
		List<Bucket> bucketsWithFormats = bucketFormatResolver
				.resolveBucketsFormats(filteredBuckets);
		return bucketsWithFormats;
	}

}
