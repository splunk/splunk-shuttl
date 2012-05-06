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
package com.splunk.shep.archiver.archive;

import static com.splunk.shep.testutil.UtilsFile.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;

@Test(groups = { "fast-unit" })
public class CsvExporterTest {

    private CsvExporter csvExporter;
    private SplunkExportTool exportTool;
    private Runtime runtime;
    private BucketCsvFile bucketCsvFile;
    private Bucket bucket;
    private ShellExecutor shellExecutor;
    private File dummyFile;

    @BeforeMethod
    public void setUp() {
	exportTool = mock(SplunkExportTool.class);
	bucketCsvFile = mock(BucketCsvFile.class);
	shellExecutor = mock(ShellExecutor.class);
	csvExporter = new CsvExporter(exportTool, bucketCsvFile, shellExecutor);

	bucket = UtilsBucket.createTestBucket();

	dummyFile = new File("/dummy/file");
    }

    public void exportBucketToCsv_givenRuntimeExportToolAndOutputDir_callsRuntimeInTheCorrectWay()
	    throws IOException {
	when(exportTool.getExecutableFile()).thenReturn(
		new File("/exporttool/path"));
	when(bucketCsvFile.getCsvFile(bucket)).thenReturn(createTestFile());
	String bucketPath = bucket.getDirectory().getAbsolutePath();

	String[] command = new String[] { "/exporttool/path", bucketPath,
		"/csv/path", "-csv" };
	when(shellExecutor.executeCommand(command)).thenReturn(0);

	csvExporter.exportBucketToCsv(bucket);
    }

    @Test(groups = { "fast-unit" }, expectedExceptions = { CsvExportFailedException.class })
    public void exportBucketToCsv_nonZeroExitStatus_throwCsvExportFailedException()
	    throws IOException, InterruptedException {
	when(shellExecutor.executeCommand((String[]) anyObject()))
		.thenReturn(1);

	when(exportTool.getExecutableFile()).thenReturn(dummyFile);
	when(bucketCsvFile.getCsvFile(bucket)).thenReturn(dummyFile);
	csvExporter.exportBucketToCsv(bucket);
    }

    @Test(groups = { "fast-unit" }, expectedExceptions = { CsvExportFailedException.class })
    public void exportBucketToCsv_csvFileDoesNotExistAfterExport_throwCsvExportFailedException() {
	File nonExistantCsvFile = createTestFile();
	when(bucketCsvFile.getCsvFile(bucket)).thenReturn(nonExistantCsvFile);

	when(shellExecutor.executeCommand((String[]) anyObject()))
		.thenReturn(0);
	when(exportTool.getExecutableFile()).thenReturn(dummyFile);

	FileUtils.deleteQuietly(nonExistantCsvFile);
	csvExporter.exportBucketToCsv(bucket);
    }
}
