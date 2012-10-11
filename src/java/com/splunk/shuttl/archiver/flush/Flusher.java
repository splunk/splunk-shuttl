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
package com.splunk.shuttl.archiver.flush;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.splunk.shuttl.archiver.listers.ArchivedIndexesLister;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.FileNotDirectoryException;
import com.splunk.shuttl.archiver.model.IllegalIndexException;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.thaw.BucketFilter;
import com.splunk.shuttl.archiver.thaw.SplunkSettings;
import com.splunk.shuttl.archiver.util.IllegalRegexGroupException;

/**
 * Removes aka flushes, buckets in an index for a time range.
 */
public class Flusher {

	private final SplunkSettings splunkSettings;
	private ArrayList<Bucket> flushedBuckets;
	private ArchivedIndexesLister indexesLister;

	/**
	 * @param splunkSettings
	 * @param indexesLister
	 */
	public Flusher(SplunkSettings splunkSettings,
			ArchivedIndexesLister indexesLister) {
		this.splunkSettings = splunkSettings;
		this.indexesLister = indexesLister;
		this.flushedBuckets = new ArrayList<Bucket>();
	}

	/**
	 * @param index
	 *          to flush
	 * @param earliest
	 * @param latest
	 * @throws IllegalIndexException
	 *           if the index is not in the archive. Protecting indexes that
	 *           aren't using Shuttl to lose their thaw data.
	 */
	public void flush(String index, Date earliest, Date latest)
			throws IllegalIndexException {
		if (!indexesLister.listIndexes().contains(index))
			throw new IllegalIndexException("Index does not exist in the archive, "
					+ "which means it cannot have been thawed.");

		try {
			doFlush(index, earliest, latest);
		} catch (IllegalIndexException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void doFlush(String index, Date earliest, Date latest)
			throws FileNotDirectoryException, IOException {
		File thawLocation = splunkSettings.getThawLocation(index);
		List<LocalBucket> buckets = ThawedBuckets.getBucketsFromThawLocation(index,
				thawLocation);
		List<LocalBucket> bucketsToFlush = filterByTimeRange(earliest, latest,
				buckets);
		for (LocalBucket b : bucketsToFlush) {
			b.deleteBucket();
			flushedBuckets.add(b);
		}
	}

	private List<LocalBucket> filterByTimeRange(Date earliest, Date latest,
			List<LocalBucket> buckets) {
		List<LocalBucket> filtered = new ArrayList<LocalBucket>();
		for (LocalBucket b : buckets) {
			try {
				if (BucketFilter.isBucketWithinTimeRange(b, earliest, latest)) {
					filtered.add(b);
				}
			} catch (IllegalRegexGroupException e) {
				// Do nothing.
			}
		}
		return filtered;
	}

	/**
	 * @return the buckets flushed.
	 */
	public List<Bucket> getFlushedBuckets() {
		return flushedBuckets;
	}
}
