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

import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.ArchiverMBeanNotRegisteredException;
import com.splunk.shuttl.testutil.TUtilsConf;
import com.splunk.shuttl.testutil.TUtilsMBean;

@Test(groups = { "fast-unit" })
public class BucketFreezerProviderTest {

	public void getConfiguredBucketFreezer_givenRegisteredMBeans_notNull() {
		File shuttlConfs = TUtilsConf.getNullConfsDir();
		TUtilsMBean.runWithRegisteredMBeans(shuttlConfs, new Runnable() {

			@Override
			public void run() {
				BucketFreezer bf = new BucketFreezerProvider()
						.getConfiguredBucketFreezer();
				assertNotNull(bf);
			}
		});
	}

	@Test(expectedExceptions = { ArchiverMBeanNotRegisteredException.class })
	public void getConfiguredBucketFreezer_notRegisteredMBeans_throwArchiverMBeanNotRegistered() {
		new BucketFreezerProvider().getConfiguredBucketFreezer();
	}
}
