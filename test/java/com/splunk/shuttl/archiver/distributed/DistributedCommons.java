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
package com.splunk.shuttl.archiver.distributed;

import java.io.File;

import com.splunk.Service;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsEndToEnd;

public class DistributedCommons {

	public static void archiveBucketAtSearchPeer(LocalBucket b, String host,
			int shuttlPort) {
		TUtilsEndToEnd.callSlaveArchiveBucketEndpoint(b.getIndex(), b
				.getDirectory().getAbsolutePath(), host, shuttlPort);
	}

	public static void cleanHadoopFileSystem(String splunkHome) {
		File shuttlConfDir = TUtilsEndToEnd
				.getShuttlConfDirFromSplunkHome(splunkHome);
		TUtilsEndToEnd.cleanHadoopFileSystem(shuttlConfDir, splunkHome);
	}

	public static LocalBucket putBucketInPeerThawDirectory(String splunkHost,
			String splunkPort, String splunkUser, String splunkPass, String index) {
		Service service = TUtilsEndToEnd.getLoggedInService(splunkHost, splunkPort,
				splunkUser, splunkPass);
		File thawDirectory = new File(service.getIndexes().get(index)
				.getThawedPathExpanded());
		return TUtilsBucket.createBucketInDirectoryWithIndex(thawDirectory, index);
	}

}
