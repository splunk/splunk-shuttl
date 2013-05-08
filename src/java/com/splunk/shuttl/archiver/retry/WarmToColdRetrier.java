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
package com.splunk.shuttl.archiver.retry;

import com.splunk.Service;
import com.splunk.WarmToColdIndex;
import com.splunk.WarmToColdIndexCollection;
import com.splunk.shuttl.archiver.archive.RegistersArchiverMBean;
import com.splunk.shuttl.archiver.copy.ColdCopyEntryPoint;
import com.splunk.shuttl.archiver.thaw.SplunkIndexedLayerFactory;
import com.splunk.shuttl.server.mbeans.JMXSplunk;
import com.splunk.shuttl.server.mbeans.JMXSplunkMBean;
import com.splunk.shuttl.server.mbeans.util.RegistersMBeans;

/**
 * Retries to copy any buckets that has failed to be transferred.
 */
public class WarmToColdRetrier implements Runnable {

	private Service service;

	public WarmToColdRetrier(Service service) {
		this.service = service;
	}

	@Override
	public void run() {
		retryAllIndexesWithShuttlsWarmToColdScriptSet();
	}

	private void retryAllIndexesWithShuttlsWarmToColdScriptSet() {
		WarmToColdIndexCollection indexCollection = new WarmToColdIndexCollection(
				service);

		for (WarmToColdIndex index : indexCollection.values())
			if (isWarmToColdScriptConfiguredToBeShuttlsScript(index))
				ColdCopyEntryPoint.createColdBucketCopier().tryCopyingColdBuckets(
						index.getName());
	}

	private boolean isWarmToColdScriptConfiguredToBeShuttlsScript(
			WarmToColdIndex index) {
		String shuttlWarmToColdScript = "etc/apps/shuttl/bin/warmToColdScript.sh";

		return index.getWarmToColdScript() != null
				&& index.getWarmToColdScript().endsWith(shuttlWarmToColdScript);
	}

	public static void main(String... args) {
		registerMBeans();

		Service service = SplunkIndexedLayerFactory.getLoggedInSplunkService();
		new WarmToColdRetrier(service).run();
	}

	private static void registerMBeans() {
		RegistersArchiverMBean.create().register();
		RegistersMBeans registersMBeans = RegistersMBeans.create();
		registersMBeans.registerMBean(JMXSplunkMBean.OBJECT_NAME, new JMXSplunk());
	}

}
