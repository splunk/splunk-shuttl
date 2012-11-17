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

import static org.testng.Assert.*;

import java.io.File;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.ArchiverMBeanNotRegisteredException;
import com.splunk.shuttl.testutil.TUtilsEnvironment;
import com.splunk.shuttl.testutil.TUtilsMBean;

public class BucketFreezerProviderTest {

	@Test(groups = { "end-to-end" })
	@Parameters(value = { "splunk.home", "shuttl.conf.dir" })
	public void getConfiguredBucketFreezer_givenRegisteredMBeans_notNull(
			final String splunkHome, final String shuttlConfDir) {
		final Runnable assertGettingNonNullInstance = new Runnable() {

			@Override
			public void run() {
				BucketFreezer bf = new BucketFreezerProvider()
						.getConfiguredBucketFreezer();
				assertNotNull(bf);
			}
		};

		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
				TUtilsMBean.runWithRegisteredMBeans(new File(shuttlConfDir),
						assertGettingNonNullInstance);
			}
		});

	}

	@Test(groups = { "fast-unit" }, expectedExceptions = { ArchiverMBeanNotRegisteredException.class })
	public void getConfiguredBucketFreezer_notRegisteredMBeans_throwArchiverMBeanNotRegistered() {
		new BucketFreezerProvider().getConfiguredBucketFreezer();
	}
}
