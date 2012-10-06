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
package com.splunk.shuttl.archiver.filesystem.glacier;

import com.amazonaws.auth.AWSCredentials;

public class AWSCredentialsImpl implements AWSCredentials {

	private final String id;
	private final String secret;
	private String endpoint;
	private String bucket;
	private String vault;

	public AWSCredentialsImpl(String id, String secret, String endpoint,
			String bucket, String vault) {
		this.id = id;
		this.secret = secret;
		this.endpoint = endpoint;
		this.bucket = bucket;
		this.vault = vault;
	}

	@Override
	public String getAWSAccessKeyId() {
		return id;
	}

	@Override
	public String getAWSSecretKey() {
		return secret;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public String getBucket() {
		return bucket;
	}

	public String getVault() {
		return vault;
	}

}
