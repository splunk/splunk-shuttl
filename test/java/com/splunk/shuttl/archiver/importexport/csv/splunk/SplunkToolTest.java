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
package com.splunk.shuttl.archiver.importexport.csv.splunk;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static java.util.Arrays.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.testutil.TUtilsEnvironment;

@Test(groups = { "fast-unit" })
public abstract class SplunkToolTest {

	private SplunkTool splunkTool;

	@BeforeMethod
	public void setUp() {
		splunkTool = getInstance();
	}

	protected abstract SplunkTool getInstance();

	@Test(groups = { "fast-unit" })
	public void getExecutableCommand_givenSplunkHome_listWithSplunkCmdToolName() {
		final File splunkHome = createDirectory();
		File bin = createDirectoryInParent(splunkHome, "bin");
		final File splunk = createFileInParent(bin, "splunk");
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME",
						splunkHome.getAbsolutePath());
				List<String> pathToExecutable = splunkTool.getExecutableCommand();
				assertEquals(
						asList(splunk.getAbsolutePath(), "cmd", splunkTool.getToolName()),
						pathToExecutable);
			}
		});
	}

	@Test(expectedExceptions = { SplunkEnvironmentNotSetException.class })
	public void getExecutableCommand_splunkHomeIsNotSet_throwException() {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				assertTrue(System.getenv("SPLUNK_HOME") == null);
				splunkTool.getExecutableCommand();
			}
		});
	}

	public void getEnvironment_splunkHomeIsSet_equalsSplunkEnvironment() {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", "something");
				assertEquals(SplunkEnvironment.getEnvironment(),
						splunkTool.getEnvironment());
			}
		});
	}

	@Test(expectedExceptions = { SplunkEnvironmentNotSetException.class })
	public void getEnvironment_default_getsAllTheSplunkEnvironments() {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				SplunkEnvironment.getEnvironment();
			}
		});
	}
}
