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
     * @param earliestTime
     *            in the time range.
     * @param latestTime
     *            in the time range.
     * @return list of buckets that's within this time range.
     */
    public List<Bucket> filterBucketsByTimeRange(List<Bucket> buckets,
	    Date earliestTime, Date latestTime) {
	// TODO Auto-generated method stub
	throw new UnsupportedOperationException();
    }

}
