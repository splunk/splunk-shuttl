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

import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.ShellExecutor;
import com.splunk.shuttl.archiver.importexport.csv.splunk.SplunkImportTool;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsEnvironment;

@Test(groups = { "end-to-end" })
public class CsvImporterFunctionalTest {

    private CsvImporter integratedCsvImporter;
    private Bucket csvBucket;

    @BeforeMethod
    public void setUp() {
	SplunkImportTool importTool = new SplunkImportTool();
	ShellExecutor shellExecutor = new ShellExecutor(Runtime.getRuntime());
	integratedCsvImporter = new CsvImporter(importTool, shellExecutor,
		new BucketFactory());
	csvBucket = TUtilsBucket.createRealCsvBucket();
    }

    @Test(groups = { "end-to-end" })
    @Parameters(value = { "splunk.home" })
    public void CsvImporter_givenSplunkHomeAndRealInstances_createSplunkBucketFromImportedBucket(
	    final String splunkHome) {
	TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

	    @Override
	    public void run() {
		TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME",
			splunkHome);
		Bucket importedBucket = integratedCsvImporter
			.importBucketFromCsv(csvBucket);
		assertFormatIsChangedOnImportedBucket(csvBucket, importedBucket);
	    }
	});
    }

    private void assertFormatIsChangedOnImportedBucket(Bucket csvBucket,
	    Bucket importedBucket) {
	assertEquals(BucketFormat.SPLUNK_BUCKET, importedBucket.getFormat());
	assertEquals(csvBucket.getName(), importedBucket.getName());
	assertEquals(csvBucket.getDirectory(), importedBucket.getDirectory());
	assertEquals(csvBucket.getIndex(), importedBucket.getIndex());
	assertEquals(csvBucket.getLatest(), importedBucket.getLatest());
	assertEquals(csvBucket.getEarliest(), importedBucket.getEarliest());
    }

}
