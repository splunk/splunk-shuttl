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

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.RegistersArchiverMBean;
import com.splunk.shuttl.archiver.model.FileNotDirectoryException;
import com.splunk.shuttl.server.mbeans.ShuttlServer;
import com.splunk.shuttl.server.mbeans.ShuttlServerMBean;

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
		String indexName = EntryPointUtil.getIndexNameForBucketDir(bucketDir);

		callCopyBucketEndpointWithBucket(indexName);
	}

	private static void callCopyBucketEndpointWithBucket(String indexName) {
		ShuttlServerMBean serverMBean = ShuttlServer
				.getRegisteredServerMBean(logger);
		RegistersArchiverMBean.create().register();

		ColdBucketCopier coldBucketCopier = createColdBucketCopier(serverMBean);

		coldBucketCopier.tryCopyingColdBuckets(indexName);
	}

	private static ColdBucketCopier createColdBucketCopier(
			ShuttlServerMBean serverMBean) {
		CallCopyBucketEndpoint callCopyBucketEndpoint = CallCopyBucketEndpoint
				.create(serverMBean);
		LocalFileSystemPaths fileSystemPaths = LocalFileSystemPaths.create();

		CopyBucketReceipts receipts = new CopyBucketReceipts(fileSystemPaths);
		ColdBucketCopier coldBucketCopier = new ColdBucketCopier(
				new ColdBucketInterator(EntryPointUtil.getSplunkService(),
						new BucketIteratorFactory()), receipts, new LockedBucketCopier(
						new CopyBucketLocker(fileSystemPaths), callCopyBucketEndpoint,
						receipts));

		return coldBucketCopier;
	}
}
