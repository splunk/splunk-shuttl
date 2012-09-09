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
import java.util.ArrayList;
import java.util.List;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.BucketFactory;

public class ThawedBuckets {

	public static List<Bucket> getBucketsFromThawLocation(String index,
			File thawLocation) {
		List<Bucket> buckets = new ArrayList<Bucket>();
		File[] files = thawLocation.listFiles();
		if (files != null)
			for (File f : files)
				if (f.isDirectory())
					buckets.add(BucketFactory.createBucketWithIndexDirectoryAndFormat(
							index, f, BucketFormat.SPLUNK_BUCKET));
		return buckets;
	}

}
