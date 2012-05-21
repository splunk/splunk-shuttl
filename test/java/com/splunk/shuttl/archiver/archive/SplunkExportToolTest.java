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
package com.splunk.shuttl.archiver.archive;

import static com.splunk.shuttl.testutil.UtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.SplunkEnrivonmentNotSetException;
import com.splunk.shuttl.archiver.archive.SplunkExportTool;
import com.splunk.shuttl.testutil.UtilsEnvironment;

@Test(groups = { "fast-unit" })
public class SplunkExportToolTest {

    SplunkExportTool splunkExportTool;

    @BeforeMethod
    public void setUp() {
	splunkExportTool = new SplunkExportTool();
    }

    @Test(expectedExceptions = { SplunkEnrivonmentNotSetException.class })
    public void getExecutableFile_noSplunkHomeEnvironmentSet_returnBucketAndLogWarning() {
	UtilsEnvironment.runInCleanEnvironment(new Runnable() {

	    @Override
	    public void run() {
		splunkExportTool.getExecutableCommand();
	    }
	});
    }

    public void getExecutableCommand_splunkHomeIsSet_commandToExecuteExportTool()
	    throws IOException {
	final File splunkHome = createTempDirectory();
	File bin = createDirectoryInParent(splunkHome, "bin");
	createFileInParent(bin, "exporttool");
	UtilsEnvironment.runInCleanEnvironment(new Runnable() {

	    @Override
	    public void run() {
		String splunkHomePath = splunkHome.getAbsolutePath();
		UtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME",
			splunkHomePath);
		String command = splunkExportTool.getExecutableCommand();
		assertEquals(command, splunkHomePath + "/bin/exporttool");
	    }
	});
    }

    public void getEnvironmentVariables_splunkHomeIsSet_getsSplunkHome() {
	UtilsEnvironment.runInCleanEnvironment(new Runnable() {

	    @Override
	    public void run() {
		UtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME",
			"/splunk/home");
		Map<String, String> keyValue = splunkExportTool
			.getEnvironmentVariables();
		assertEquals("/splunk/home", keyValue.get("SPLUNK_HOME"));
	    }
	});
    }
}
