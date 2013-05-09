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
package com.splunk;

import static org.testng.Assert.*;

import java.io.File;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.thaw.SplunkIndexedLayerFactory;
import com.splunk.shuttl.testutil.TUtilsEndToEnd;
import com.splunk.shuttl.testutil.TUtilsMBean;

@Test(groups = { "end-to-end" })
public class WarmToColdIndexCollectionTest {

	@Parameters(value = { "shuttl.conf.dir" })
	public void _givenOneIndexWithWarmToColdScript_canGetThatWarmToColdScript(
			String shuttlConfDir) {
		TUtilsMBean.runWithRegisteredMBeans(new File(shuttlConfDir),
				new Runnable() {
					@Override
					public void run() {
						doTestWithConfiguredEnvironment();
					}
				});
	}

	private void doTestWithConfiguredEnvironment() {
		WarmToColdIndexCollection indexCollection = new WarmToColdIndexCollection(
				SplunkIndexedLayerFactory.getLoggedInSplunkService());

		assertFalse(indexCollection.values().isEmpty());
		assertShuttlIndexHasWarmToColdScript(indexCollection);
		assertAllIndexesOtherThanShuttlIndexDoesNotHaveWarmToColdScript(indexCollection);
	}

	private void assertShuttlIndexHasWarmToColdScript(
			WarmToColdIndexCollection indexCollection) {
		WarmToColdIndex shuttlIndex = indexCollection
				.get(TUtilsEndToEnd.REAL_SPLUNK_INDEX);
		assertNotNull(shuttlIndex);
		assertNotNull(shuttlIndex.getWarmToColdScript());
	}

	private void assertAllIndexesOtherThanShuttlIndexDoesNotHaveWarmToColdScript(
			WarmToColdIndexCollection indexCollection) {
		for (WarmToColdIndex index : indexCollection.values())
			if (!index.getName().equals(TUtilsEndToEnd.REAL_SPLUNK_INDEX))
				assertNull(index.getWarmToColdScript());
	}
}
