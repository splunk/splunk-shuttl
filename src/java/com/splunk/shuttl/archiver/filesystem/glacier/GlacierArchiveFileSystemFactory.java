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

import java.net.URI;

/**
 * Construction logic for creating {@link GlacierArchiveFileSystem}
 */
public class GlacierArchiveFileSystemFactory {

	/**
	 * @param uri
	 *          - Has the format of glacier://ID:SECRET@ENDPOINT/VAULT
	 * 
	 * @throws UnsupportedGlacierUriException
	 *           - if the format is not valid.
	 */
	public static GlacierArchiveFileSystem create(URI uri) {
		CredentialsParser parser = new CredentialsParser(uri).parse();
		return new GlacierArchiveFileSystem(parser.id, parser.secret,
				parser.endpoint, parser.vault);
	}

	private static class CredentialsParser {

		public static final String LEGAL_URI_REGEX = "glacier://(.+?):(.+?)@(.+?)/(.+)";

		private final URI uri;
		public String id = "";
		public String secret = "";
		public String endpoint = "";
		public String vault = "";

		public CredentialsParser(URI uri) {
			this.uri = uri;
		}

		public CredentialsParser parse() {
			return this;
		}

	}

}
