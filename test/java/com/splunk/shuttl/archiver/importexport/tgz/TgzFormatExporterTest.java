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
package com.splunk.shuttl.archiver.importexport.tgz;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;

import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.BucketFileCreator;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "slow-unit" })
public class TgzFormatExporterTest {

	public void __createsATgzThenCreatesTheBucketObject() {
		CreatesBucketTgz createsBucketTgz = CreatesBucketTgz
				.create(createDirectory());
		BucketFileCreator bucketFileCreator = BucketFileCreator.createForTgz();

		TgzFormatExporter toTgz = new TgzFormatExporter(createsBucketTgz,
				bucketFileCreator);

		Bucket bucket = TUtilsBucket.createBucket();
		Bucket tgzBucket = toTgz.exportBucket(bucket);

		assertEquals(BucketFormat.SPLUNK_BUCKET_TGZ, tgzBucket.getFormat());
		File bucketDir = tgzBucket.getDirectory();
		assertTrue(bucketDir.exists());
		File[] files = bucketDir.listFiles();
		assertEquals(1, files.length);
		assertTrue(files[0].getName().endsWith("tgz"));
	}
}
