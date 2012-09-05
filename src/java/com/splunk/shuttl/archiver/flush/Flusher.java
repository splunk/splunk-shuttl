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

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.archiver.model.FileNotDirectoryException;
import com.splunk.shuttl.archiver.model.IllegalBucketNameException;
import com.splunk.shuttl.archiver.model.IllegalIndexException;
import com.splunk.shuttl.archiver.thaw.BucketFilter;
import com.splunk.shuttl.archiver.thaw.SplunkSettings;

/**
 * Removes aka flushes, buckets in an index for a time range.
 */
public class Flusher {

	private final SplunkSettings splunkSettings;
	private ArrayList<Bucket> flushedBuckets;

	/**
	 * @param splunkSettings
	 */
	public Flusher(SplunkSettings splunkSettings) {
		this.splunkSettings = splunkSettings;
		this.flushedBuckets = new ArrayList<Bucket>();
	}

	/**
	 * @param index
	 * @param earliest
	 * @param latest
	 */
	public void flush(String index, Date earliest, Date latest) {
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
		List<Bucket> buckets = getBucketsFromThawLocation(index, thawLocation);
		List<Bucket> bucketsToFlush = filterByTimeRange(earliest, latest, buckets);
		for (Bucket b : bucketsToFlush) {
			b.deleteBucket();
			flushedBuckets.add(b);
		}
	}

	private List<Bucket> getBucketsFromThawLocation(String index,
			File thawLocation) {
		List<Bucket> buckets = new ArrayList<Bucket>();
		File[] files = thawLocation.listFiles();
		if (files != null)
			for (File f : files)
				if (f.isDirectory())
					buckets.add(BucketFactory.createBucketWithIndexDirectoryAndFormat(
							index, f, BucketFormat.UNKNOWN));
		return buckets;
	}

	private List<Bucket> filterByTimeRange(Date earliest, Date latest,
			List<Bucket> buckets) {
		List<Bucket> filtered = new ArrayList<Bucket>();
		for (Bucket b : buckets) {
			try {
				if (BucketFilter.isBucketWithinTimeRange(b, earliest, latest)) {
					filtered.add(b);
				}
			} catch (IllegalBucketNameException e) {
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
