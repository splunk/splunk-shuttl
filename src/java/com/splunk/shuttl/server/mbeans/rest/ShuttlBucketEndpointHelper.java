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
package com.splunk.shuttl.server.mbeans.rest;

import com.splunk.shuttl.archiver.bucketlock.BucketLocker;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.server.mbeans.rest.ShuttlBucketEndpoint.BucketModifier;
import com.splunk.shuttl.server.mbeans.rest.ShuttlBucketEndpoint.ConfigProvider;
import com.splunk.shuttl.server.mbeans.rest.ShuttlBucketEndpoint.ShuttlProvider;

public class ShuttlBucketEndpointHelper {

	public static void shuttlBucket(String path, String index,
			ShuttlProvider shuttlProvider, ConfigProvider configProvider,
			BucketModifier bucketModifier, BucketLocker bucketLocker) {
		BucketFactory bucketFactory = new BucketFactory();
		ShuttlBucketEndpoint shuttlBucketEndpoint = new ShuttlBucketEndpoint(
				shuttlProvider, configProvider, bucketModifier, bucketFactory,
				bucketLocker);

		shuttlBucketEndpoint.shuttlBucket(path, index);
	}
}
