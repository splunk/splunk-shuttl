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
package com.splunk.shuttl.archiver;

import static org.testng.Assert.*;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.testutil.TUtilsEnvironment;

public class ConfigurationPathsTest {

	@Test(groups = { "end-to-end" })
	@Parameters(value = { "splunk.home" })
	public void _givenSplunkHome_gettingDirectoriesGetsExistingDirs(
			final String splunkHome) {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
				assertTrue(ConfigurationPaths.getDefaultConfDirectory().exists());
				assertTrue(ConfigurationPaths.getBackendConfigDirectory().exists());
			}
		});
	}

	@Test(groups = { "fast-unit" }, expectedExceptions = { RuntimeException.class })
	public void _givenNoSplunkHome_throws() {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				ConfigurationPaths.getBackendConfigDirectory();
			}
		});
	}
}
