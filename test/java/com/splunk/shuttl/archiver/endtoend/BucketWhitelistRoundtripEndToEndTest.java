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
package com.splunk.shuttl.archiver.endtoend;

import static org.testng.AssertJUnit.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.usecases.FormatRoundtripFunctionalTest;
import com.splunk.shuttl.testutil.TUtilsEnvironment;

public class BucketWhitelistRoundtripEndToEndTest extends
		FormatRoundtripFunctionalTest {

	@Override
	protected BucketFormat getFormat() {
		return BucketFormat.SPLUNK_BUCKET_LIGHT;
	}

	@Override
	protected Map<BucketFormat, Map<String, String>> getFormatMetadata() {
		HashMap<String, String> meta = new HashMap<String, String>();
		meta.put("whitelist", "journal.gz");

		Map<BucketFormat, Map<String, String>> formatMeta = new HashMap<BucketFormat, Map<String, String>>();
		formatMeta.put(getFormat(), meta);
		return formatMeta;
	}

	@Test(groups = { "end-to-end" })
	@Parameters(value = { "splunk.home" })
	public void _(String splunkHome) throws IOException {
		List<Bucket> buckets = _givenConfigWithSomeFormat_archivesBucketWithTheFormat(splunkHome);
		FileSystem fs = FileSystem.getLocal(new Configuration());
		int archiveMetaDir = 1;
		boolean madeAllAssertions = false;
		for (Bucket b : buckets) {
			Path bucketPath = new Path(b.getPath());
			FileStatus[] files = fs.listStatus(bucketPath);
			assertEquals(1 + archiveMetaDir, files.length);
			for (FileStatus status : files) {
				if (status.getPath().getName().equals("rawdata")) {
					FileStatus[] rawdataFiles = fs.listStatus(status.getPath());
					assertEquals(1, rawdataFiles.length);
					assertEquals("journal.gz", rawdataFiles[0].getPath().getName());
					madeAllAssertions = true;
				}
			}
		}
		assertTrue(madeAllAssertions);
	}

	@Test(groups = { "end-to-end" }, enabled = false)
	@Parameters(value = { "splunk.home" })
	public void _2(final String splunkHome) {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
				_givenConfigWithSomeFormat_thawsBucketToSplunkBucket(splunkHome);
			}
		});
	}
}
