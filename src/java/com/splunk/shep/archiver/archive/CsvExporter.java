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

import static com.splunk.shep.archiver.LogFormatter.*;

import java.io.File;

import org.apache.log4j.Logger;

import com.splunk.shep.archiver.model.Bucket;

/**
 * Exports a bucket to .csv format.
 */
public class CsvExporter {

    private static final Logger logger = Logger.getLogger(CsvExporter.class);

    private final SplunkExportTool exportTool;
    private final BucketCsvFile bucketCsvFile;
    private final ShellExecutor shellExecutor;

    public CsvExporter(SplunkExportTool exportTool,
	    BucketCsvFile bucketCsvFile, ShellExecutor shellExecutor) {
	this.exportTool = exportTool;
	this.bucketCsvFile = bucketCsvFile;
	this.shellExecutor = shellExecutor;
    }

    /**
     * @return Csv file of the {@link Bucket}
     */
    public File exportBucketToCsv(Bucket bucket) {
	File csvFile = bucketCsvFile.getCsvFile(bucket);
	String[] command = constructCommand(bucket, csvFile);
	int exit = shellExecutor.executeCommand(command);
	handleExportFailures(csvFile, exit);
	return csvFile;
    }

    private String[] constructCommand(Bucket bucket, File csvFile) {
	File exportToolExecutable = exportTool.getExecutableFile();
	String[] command = new String[] {
		exportToolExecutable.getAbsolutePath(),
		bucket.getDirectory().getAbsolutePath(),
		csvFile.getAbsolutePath(), "-csv" };
	return command;
    }

    private void handleExportFailures(File csvFile, int exit) {
	if (exit != 0) {
	    logger.debug(did("Exported a bucket to Csv",
		    "Got a non zero exit code from export tool",
		    "Zero exit code from export tool.", "exit_code", exit,
		    "csv_file", csvFile));
	    throw new CsvExportFailedException("Exporttool exited with"
		    + " non zero exit status.");
	} else if (!csvFile.exists()) {
	    logger.debug(did("Exported a bucket to Csv",
		    "Csv file didn't exist after the export",
		    "The csv file to exist", "csv_file", csvFile));
	    throw new CsvExportFailedException("Csv file didn't exist after "
		    + "the export. Something went wrong.");
	}
    }

    /**
     * @return a CsvExporter
     */
    public static CsvExporter get() {
	return new CsvExporter(new SplunkExportTool(),
		new BucketCsvFile(), ShellExecutor.getInstance());
    }

}
