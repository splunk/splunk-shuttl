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
package com.splunk.shuttl.archiver.importexport.csv;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.File;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.importexport.GetsBucketsExportFile;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class GetsBucketsCsvFileTest {

	private GetsBucketsExportFile getsBucketsExportFile;
	private Bucket bucket;

	@BeforeMethod
	public void setUp() {
		getsBucketsExportFile = new GetsBucketsExportFile(new LocalFileSystemPaths(
				createDirectory()));
		bucket = TUtilsBucket.createBucket();
	}

	public void getCsvOuputFileFromBucket_givenBucket_hasCsvExtension() {
		File csvFile = getsBucketsExportFile.getCsvFile(bucket);
		assertTrue(csvFile.getName().endsWith(".csv"));
	}

}
