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

import static java.util.Arrays.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.ShellExecutor;
import com.splunk.shuttl.archiver.importexport.csv.CsvImporter;
import com.splunk.shuttl.archiver.importexport.csv.splunk.SplunkImportTool;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.util.UtilsBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class CsvImporterTest {

    private CsvImporter csvImporter;
    private SplunkImportTool splunkImportTool;
    private Map<String, String> emptyMap;
    private ShellExecutor shellExecutor;

    @BeforeMethod
    public void setUp() {
	splunkImportTool = mock(SplunkImportTool.class);
	shellExecutor = mock(ShellExecutor.class);
	csvImporter = new CsvImporter(splunkImportTool, shellExecutor);

	emptyMap = Collections.<String, String> emptyMap();
    }

    @Test(groups = { "fast-unit" })
    public void _givenCsvBucket_callsSplunkImportToolWithCsvFile() {
	Bucket csvBucket = TUtilsBucket.createRealCsvBucket();
	String importToolPath = "path/to/importtool";
	when(splunkImportTool.getExecutableCommand())
		.thenReturn(importToolPath);
	when(splunkImportTool.getEnvironment()).thenReturn(emptyMap);
	File csvFile = UtilsBucket.getCsvFile(csvBucket);
	String[] fullCommand = new String[] { importToolPath,
		csvBucket.getDirectory().getAbsolutePath(), // <-- should move
							    // this logic
							    // somewhere.
		csvFile.getAbsolutePath() };

	csvImporter.importBucketFromCsv(csvBucket);

	verify(shellExecutor).executeCommand(emptyMap, asList(fullCommand));
    }

    @SuppressWarnings("unchecked")
    public void _givenSuccessfulImport_removeCsvFile() {
	Bucket csvBucket = TUtilsBucket.createRealCsvBucket();
	File csvFile = UtilsBucket.getCsvFile(csvBucket);
	assertTrue(csvFile.exists());
	when(shellExecutor.executeCommand(anyMap(), anyList())).thenReturn(0);

	csvImporter.importBucketFromCsv(csvBucket);
	assertFalse(csvFile.exists());
    }

    @SuppressWarnings("unchecked")
    public void _givenUnsuccessfulImport_dontRemoveCsvFile() {
	Bucket csvBucket = TUtilsBucket.createRealCsvBucket();
	File csvFile = UtilsBucket.getCsvFile(csvBucket);
	when(shellExecutor.executeCommand(anyMap(), anyList())).thenReturn(1);

	assertTrue(csvFile.exists());
	csvImporter.importBucketFromCsv(csvBucket);
	assertTrue(csvFile.exists());
    }

    @Test(expectedExceptions = { IllegalArgumentException.class })
    public void _bucketNotInCsvFormat_throwsIllegalArgumentException() {
	Bucket nonCsvBucket = TUtilsBucket.createTestBucket();
	assertNotEquals(BucketFormat.CSV, nonCsvBucket.getFormat());
	csvImporter.importBucketFromCsv(nonCsvBucket);
    }
}
