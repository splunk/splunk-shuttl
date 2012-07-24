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
package com.splunk.shuttl.archiver.archive;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketFormatTest {

	@Test(groups = { "fast-unit" })
	public void getFormatFromDirectory_emptyDirectory_unknownFormat() {
		BucketFormat format = BucketFormat
				.getFormatFromDirectory(createDirectory());
		assertEquals(BucketFormat.UNKNOWN, format);
	}

	@Test(groups = { "slow-unit" })
	public void getFormatFromDirectory_givenRealSplunkBucket_SplunkBucketFormat() {
		Bucket realSplunkBucket = TUtilsBucket.createRealBucket();
		assertEquals(BucketFormat.SPLUNK_BUCKET, realSplunkBucket.getFormat());

		BucketFormat format = BucketFormat.getFormatFromDirectory(realSplunkBucket
				.getDirectory());
		assertEquals(BucketFormat.SPLUNK_BUCKET, format);
	}

	@Test(groups = { "slow-unit" })
	public void getFormatFromDirectory_givenRealCsvBucket_CsvBucketFormat() {
		Bucket csvBucket = TUtilsBucket.createRealCsvBucket();
		assertEquals(BucketFormat.CSV, csvBucket.getFormat());

		BucketFormat format = BucketFormat.getFormatFromDirectory(csvBucket
				.getDirectory());
		assertEquals(BucketFormat.CSV, format);
	}
}
