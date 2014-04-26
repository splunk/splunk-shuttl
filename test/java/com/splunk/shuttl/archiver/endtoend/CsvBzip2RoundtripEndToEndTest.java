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

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.usecases.FormatRoundtripFunctionalTest;
import com.splunk.shuttl.testutil.TUtilsEnvironment;

public class CsvBzip2RoundtripEndToEndTest extends
		FormatRoundtripFunctionalTest {

	@Override
	protected BucketFormat getFormat() {
		return BucketFormat.CSV_BZIP2;
	}

	@Test(groups = { "fast-test" })
	public void _givenBzip2Codec_isSplittableCompressionCodec() {
		BZip2Codec bZip2Codec = new BZip2Codec();
		assertTrue(bZip2Codec instanceof SplittableCompressionCodec);
	}
	
	@Test(groups = { "end-to-end" })
	@Parameters(value = { "splunk.home" })
	public void _givenConfigWithTgzFormat_archivesTgzBucket(String splunkHome)
			throws Exception {
		_givenConfigWithSomeFormat_archivesBucketWithTheFormat(splunkHome);
	}

	@Test(groups = { "end-to-end" })
	@Parameters(value = { "splunk.home" })
	public void _givenConfigWithTgzFormat_thawsBucketToSplunkBucket(
			final String splunkHome) throws Exception {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
				_givenConfigWithSomeFormat_thawsBucketToSplunkBucket(splunkHome);
			}
		});
	}
}
