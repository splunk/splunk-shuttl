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
import static java.util.Arrays.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.importexport.GetsBucketsExportFile;
import com.splunk.shuttl.archiver.importexport.ShellExecutor;
import com.splunk.shuttl.archiver.importexport.csv.splunk.SplunkExportTool;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketToCsvFileExporterTest {

	private BucketToCsvFileExporter bucketToCsvFileExporter;
	private SplunkExportTool exportTool;
	private GetsBucketsExportFile getsBucketsExportFile;
	private LocalBucket bucket;
	private ShellExecutor shellExecutor;
	private Map<String, String> emptyMap;

	@BeforeMethod
	public void setUp() {
		exportTool = mock(SplunkExportTool.class);
		getsBucketsExportFile = mock(GetsBucketsExportFile.class);
		shellExecutor = mock(ShellExecutor.class);
		bucketToCsvFileExporter = new BucketToCsvFileExporter(exportTool, getsBucketsExportFile,
				shellExecutor);

		bucket = TUtilsBucket.createBucket();
		emptyMap = Collections.<String, String> emptyMap();
	}

	public void exportBucketToCsv_givenExecutableCommandEnvironmentAndCsvExportPath_executesSpecifiedCommand()
			throws IOException {
		when(exportTool.getExecutableCommand()).thenReturn(
				asList("/exporttool/path"));
		when(exportTool.getEnvironment()).thenReturn(emptyMap);
		File csvFile = createFile();
		when(getsBucketsExportFile.getCsvFile(bucket)).thenReturn(csvFile);
		String bucketPath = bucket.getDirectory().getAbsolutePath();

		bucketToCsvFileExporter.exportBucketToCsv(bucket);

		String[] command = new String[] { "/exporttool/path", bucketPath,
				csvFile.getAbsolutePath(), "-csv" };
		verify(shellExecutor).executeCommand(emptyMap, asList(command));
	}

	@SuppressWarnings("unchecked")
	@Test(groups = { "fast-unit" }, expectedExceptions = { CsvExportFailedException.class })
	public void exportBucketToCsv_nonZeroExitStatus_throwCsvExportFailedException()
			throws IOException, InterruptedException {
		when(shellExecutor.executeCommand(anyMap(), anyList())).thenReturn(1);

		when(exportTool.getExecutableCommand()).thenReturn(
				asList("/exporttool/path"));
		when(getsBucketsExportFile.getCsvFile(bucket)).thenReturn(
				new File("/dummy/file"));
		bucketToCsvFileExporter.exportBucketToCsv(bucket);
	}

	@SuppressWarnings("unchecked")
	@Test(groups = { "fast-unit" }, expectedExceptions = { CsvExportFailedException.class })
	public void exportBucketToCsv_csvFileDoesNotExistAfterExport_throwCsvExportFailedException() {
		File nonExistantCsvFile = createFile();
		when(getsBucketsExportFile.getCsvFile(bucket)).thenReturn(
				nonExistantCsvFile);

		when(shellExecutor.executeCommand(anyMap(), anyList())).thenReturn(0);
		when(exportTool.getExecutableCommand()).thenReturn(
				asList("/exporttool/path"));

		FileUtils.deleteQuietly(nonExistantCsvFile);
		bucketToCsvFileExporter.exportBucketToCsv(bucket);
	}
}
