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

import static com.splunk.shuttl.ShuttlConstants.*;

import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketShuttler;
import com.splunk.shuttl.archiver.archive.BucketShuttlerFactory;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.server.mbeans.rest.ShuttlBucketEndpointHelper.BucketModifier;
import com.splunk.shuttl.server.mbeans.rest.ShuttlBucketEndpointHelper.ConfigProvider;
import com.splunk.shuttl.server.mbeans.rest.ShuttlBucketEndpointHelper.ShuttlProvider;

@Path(ENDPOINT_ARCHIVER + ENDPOINT_BUCKET_COPY)
public class CopyBucketEndpoint {

	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public void copyBucket(@FormParam("path") String path,
			@FormParam("index") String index) {
		ShuttlBucketEndpointHelper.shuttlBucket(path, index,
				new BucketCopierProvider(), new NormalSharedConfigProvider(),
				new NoOpBucketModifier());
	}

	private static class BucketCopierProvider implements ShuttlProvider {

		@Override
		public BucketShuttler createWithConfig(ArchiveConfiguration config) {
			return BucketShuttlerFactory.createCopierWithConfig(config);
		}
	}

	private static class NormalSharedConfigProvider implements ConfigProvider {

		@Override
		public ArchiveConfiguration createWithBucket(LocalBucket bucket) {
			return ArchiveConfiguration.getSharedInstance();
		}
	}

	private static class NoOpBucketModifier implements BucketModifier {

		@Override
		public LocalBucket modifyLocalBucket(LocalBucket bucket) {
			return bucket;
		}
	}
}
