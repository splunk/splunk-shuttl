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
package com.splunk.shuttl.archiver.copy;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.splunk.Service;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.FileNotDirectoryException;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.thaw.SplunkSettingsFactory;
import com.splunk.shuttl.server.mbeans.JMXSplunk;
import com.splunk.shuttl.server.mbeans.JMXSplunkMBean;
import com.splunk.shuttl.server.mbeans.ShuttlServer;
import com.splunk.shuttl.server.mbeans.ShuttlServerMBean;
import com.splunk.shuttl.server.mbeans.util.RegistersMBeans;

public class ColdCopyEntryPoint {

	private static final Logger logger = Logger
			.getLogger(ColdCopyEntryPoint.class);

	public static void main(String[] args) throws FileNotFoundException,
			FileNotDirectoryException {
		try {
			File bucketDir = new File(args[0]);
			execute(bucketDir);
		} catch (Throwable t) {
			logger.error(did("Called main entry point for copying bucket", t,
					"to eventually call copy bucket REST endpoint", "main_args",
					Arrays.toString(args)));
		}
	}

	private static void execute(File bucketDir) throws FileNotFoundException {
		String indexName = getIndexNameForBucketDir(bucketDir);
		LocalBucket bucket = new LocalBucket(bucketDir, indexName,
				BucketFormat.SPLUNK_BUCKET);

		callCopyBucketEndpointWithBucket(bucket);
	}

	private static String getIndexNameForBucketDir(File bucketDir) {
		Service splunkService = getSplunkService();
		return IndexScanner.getIndexNameByBucketPath(bucketDir, splunkService);
	}

	private static Service getSplunkService() {
		RegistersMBeans.create().registerMBean(JMXSplunkMBean.OBJECT_NAME,
				new JMXSplunk());
		return SplunkSettingsFactory.getLoggedInSplunkService();
	}

	private static void callCopyBucketEndpointWithBucket(LocalBucket bucket) {
		ShuttlServerMBean serverMBean = ShuttlServer
				.getRegisteredServerMBean(logger);

		CallCopyBucketEndpoint callCopyBucketEndpoint = CallCopyBucketEndpoint
				.create(serverMBean);

		callCopyBucketEndpoint.call(bucket);
	}

}
