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

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;
import com.splunk.shep.testutil.UtilsEnvironment;

@Test(groups = { "fast" })
public class SplunkExportToolTest {

    private SplunkExportTool splunkExportTool;
    private Runtime runtime;

    @BeforeMethod
    public void setUp() {
	runtime = mock(Runtime.class);
	splunkExportTool = new SplunkExportTool(runtime);
    }

    @Test(expectedExceptions = { SplunkEnrivonmentNotSetException.class })
    public void exportToCsv_noSplunkHomeEnvironmentSet_returnBucketAndLogWarning() {
	final Bucket bucket = mock(Bucket.class);
	UtilsEnvironment.runInCleanEnvironment(new Runnable() {

	    @Override
	    public void run() {
		splunkExportTool.exportToCsv(bucket);
	    }
	});
    }

    public void exportToCsv_splunkHomeIsSet_callExportToolWithBucket()
	    throws IOException {
	final Bucket bucket = UtilsBucket.createTestBucket();
	final String splunkHome = "/any/path";
	String bucketPath = bucket.getDirectory().getAbsolutePath();
	final String csvOutput = bucket.getDirectory().getParentFile()
		.getAbsolutePath()
		+ "/bucket.csv";
	String[] commandsToCallExportTool = new String[] {
		splunkHome + "/bin/exporttool", bucketPath, csvOutput, "-csv" };
	UtilsEnvironment.runInCleanEnvironment(new Runnable() {

	    @Override
	    public void run() {
		UtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME",
			"/any/path");
		assertEquals(new File(csvOutput),
			splunkExportTool.exportToCsv(bucket));
	    }
	});
	verify(runtime).exec(commandsToCallExportTool);
    }
}
