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
package com.splunk.shuttl.archiver.filesystem.s3;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.glacier.AWSCredentialsImpl;
import com.splunk.shuttl.archiver.filesystem.hadoop.HadoopArchiveFileSystem;

/**
 * Factory for creating an AWS S3 or S3n back-end.
 */
public class S3ArchiveFileSystemFactory {

	/**
	 * @return back-end running S3.
	 */
	public static ArchiveFileSystem createS3() {
		return create("s3");
	}

	/**
	 * @return back-end running S3n.
	 */
	public static ArchiveFileSystem createS3n() {
		return create("s3n");
	}

	private static ArchiveFileSystem create(String scheme) {
		URI s3Uri = createS3UriForHadoopFileSystem(scheme);

		try {
			return new HadoopArchiveFileSystem(FileSystem.get(s3Uri,
					new Configuration()));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static URI createS3UriForHadoopFileSystem(String scheme) {
		AWSCredentialsImpl credentials = AWSCredentialsImpl.create();
		return URI.create(scheme + "://"
				+ urlEncode(credentials.getAWSAccessKeyId()) + ":"
				+ urlEncode(credentials.getAWSSecretKey()) + "@"
				+ urlEncode(credentials.getS3Bucket()));
	}

	private static String urlEncode(String s) {
		try {
			return URLEncoder.encode(s, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
}
