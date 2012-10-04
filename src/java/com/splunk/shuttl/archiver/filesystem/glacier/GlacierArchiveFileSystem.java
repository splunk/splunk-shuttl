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

import java.io.File;
import java.io.IOException;
import java.net.URI;

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.hadoop.HadoopArchiveFileSystem;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * The glacier file system is not good for storing multiple files, it will
 * therefore rely on s3 to handle the storing of meta data and file structure.
 * It supports only buckets that contain a single file.
 */
public class GlacierArchiveFileSystem extends HadoopArchiveFileSystem implements
		ArchiveFileSystem {

	private String id;
	private String secret;
	private String endpoint;
	private String vault;

	/**
	 * @param id
	 * @param secret
	 * @param endpoint
	 * @param vault
	 */
	public GlacierArchiveFileSystem(String id, String secret, String endpoint,
			String vault) {
		super(null);
		this.id = id;
		this.secret = secret;
		this.endpoint = endpoint;
		this.vault = vault;
	}

	@Override
	public void putBucket(Bucket localBucket, URI temp, URI dst)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void getBucket(Bucket remoteBucket, File temp, File dst)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void getFile(URI src, File temp, File dst) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanBucketTransaction(Bucket bucket, URI temp) {
		super.cleanBucketTransaction(bucket, temp);
	}

	public String getId() {
		return id;
	}

	public String getSecret() {
		return secret;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public String getVault() {
		return vault;
	}

}
