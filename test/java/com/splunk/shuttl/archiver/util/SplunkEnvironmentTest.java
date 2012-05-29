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
package com.splunk.shuttl.archiver.util;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.importexport.csv.splunk.SplunkEnvironment;
import com.splunk.shuttl.archiver.importexport.csv.splunk.SplunkEnvironmentNotSetException;
import com.splunk.shuttl.testutil.TUtilsEnvironment;

@Test(groups = { "fast-unit" })
public class SplunkEnvironmentTest {

	public void getSplunkHome_splunkHomeIsSet_commandToExecuteExportTool()
			throws IOException {
		final File splunkHome = createTempDirectory();
		File bin = createDirectoryInParent(splunkHome, "bin");
		createFileInParent(bin, "exporttool");
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				String splunkHomePath = splunkHome.getAbsolutePath();
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHomePath);
				assertEquals(splunkHomePath, SplunkEnvironment.getSplunkHome());
			}
		});
	}

	@Test(expectedExceptions = { SplunkEnvironmentNotSetException.class })
	public void getSplunkHome_noSplunkHomeEnvironmentSet_thrownException() {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				SplunkEnvironment.getSplunkHome();
			}
		});
	}

	public void getEnvironmentVariables_splunkHomeIsSet_getsSplunkHome() {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", "/splunk/home");
				Map<String, String> keyValue = SplunkEnvironment.getEnvironment();
				assertEquals("/splunk/home", keyValue.get("SPLUNK_HOME"));
			}
		});
	}

	@Test(expectedExceptions = { SplunkEnvironmentNotSetException.class })
	public void getEnvironmentVariables_splunkHomeIsNotSet_throwsException() {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				SplunkEnvironment.getEnvironment();
			}
		});
	}
}
