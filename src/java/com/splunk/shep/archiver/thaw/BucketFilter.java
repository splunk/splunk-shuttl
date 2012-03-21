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

import static com.splunk.shep.archiver.ArchiverLogger.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.splunk.shep.archiver.model.Bucket;

/**
 * Used for filter buckets.
 */
public class BucketFilter {

    /**
     * Filters buckets by time range. Returns a list that satisfies the
     * condition where it's within the time range.
     * 
     * @param buckets
     *            to filter
     * @param earliest
     *            in the time range.
     * @param latest
     *            in the time range.
     * @return list of buckets that's within this time range.
     */
    public List<Bucket> filterBucketsByTimeRange(List<Bucket> buckets,
	    Date earliest, Date latest) {
	if (earliest.after(latest)) {
	    warn("Filtered buckets by time range",
		    "Earliest time was later than latest time",
		    "Filtered all buckets", "earliest_time", earliest,
		    "latest_time", latest);
	    return Arrays.asList();
	}
	ArrayList<Bucket> filteredBuckets = new ArrayList<Bucket>();
	for (Bucket bucket : buckets) {
	    if (isBucketWithinTimeRange(bucket, earliest, latest)) {
		filteredBuckets.add(bucket);
	    }
	}
	return filteredBuckets;
    }

    private boolean isBucketWithinTimeRange(Bucket bucket, Date earliest,
	    Date latest) {
	if (bucket.getLatest().before(earliest))
	    return false;
	if (bucket.getEarliest().after(latest))
	    return false;
	return true;
    }

}
