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

import com.splunk.shuttl.archiver.importexport.GetsBucketsExportFile;
import com.splunk.shuttl.archiver.importexport.ShellExecutor;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "slow-unit" })
public class CreatesBucketTgzIntegrationTest {

	public void _usingRealClasses_tgzBucketFileExists() {
		CreatesBucketTgz createsBucketTgz = new CreatesBucketTgz(
				ShellExecutor.getInstance(), new GetsBucketsExportFile(
						createDirectory()));

		Bucket bucket = TUtilsBucket.createBucket();
		File tgz = createsBucketTgz.createTgz(bucket);
		assertTrue(tgz.exists());
		assertNotEquals(0, tgz.length());
	}
}
