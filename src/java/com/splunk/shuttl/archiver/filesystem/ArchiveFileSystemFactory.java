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
package com.splunk.shuttl.archiver.filesystem;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.filesystem.glacier.GlacierArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.filesystem.hadoop.HadoopArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.hadoop.HadoopArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.filesystem.s3.S3ArchiveFileSystemFactory;

/**
 * Factory for getting an {@link ArchiveFileSystem}
 */
public class ArchiveFileSystemFactory {

	public static final String LOCAL_FILESYSTEM_BACKEND_NAME = "local";
	private static final Logger logger = Logger
			.getLogger(ArchiveFileSystemFactory.class);
	private static final Set<String> supportedBackends;

	static {
		supportedBackends = new HashSet<String>();
		supportedBackends.add(LOCAL_FILESYSTEM_BACKEND_NAME);
		supportedBackends.add("hdfs");
		supportedBackends.add("s3");
		supportedBackends.add("s3n");
		supportedBackends.add("glacier");
	}

	/**
	 * @return true if {@link ArchiveFileSystemFactory} can create an
	 *         {@link ArchiveFileSystem} from that URI.
	 */
	public static boolean isSupportedBackend(String backend) {
		return supportedBackends.contains(backend);
	}

	/**
	 * @return {@link ArchiveFileSystem} as configured.
	 */
	public static ArchiveFileSystem getConfiguredArchiveFileSystem() {
		ArchiveConfiguration config = ArchiveConfiguration.getSharedInstance();
		return getWithConfiguration(config);
	}

	/**
	 * Method that is needed when mocking a configuration for tests.
	 * 
	 * @return {@link ArchiveFileSystem} with a specific
	 *         {@link ArchiveConfiguration}.
	 */
	public static ArchiveFileSystem getWithConfiguration(
			ArchiveConfiguration config) {
		return getByNameAndLocalFileSystemPaths(config.getBackendName(),
				LocalFileSystemPaths.create(config));
	}

	/**
	 * Creates a {@link ArchiveFileSystem} for a URI to tmp path.</br> Example:
	 * 'file:/tmp' contains the scheme and the path to a tmp directory.</br>
	 * 'hdfs://localhost:1234/archive-tmp' contains scheme, host, port and path to
	 * directory.
	 * 
	 * @return
	 */
	public static ArchiveFileSystem getByNameAndLocalFileSystemPaths(
			String backend, LocalFileSystemPaths localFileSystemPaths) {
		if (!supportedBackends.contains(backend))
			throw new UnsupportedBackendException("Supported backends are: "
					+ supportedBackends + ", backend was: " + backend);
		else
			return supportedArchiveFileSystem(backend, localFileSystemPaths);
	}

	private static ArchiveFileSystem supportedArchiveFileSystem(String backend,
			LocalFileSystemPaths localFileSystemPaths) {
		if (backend.equals(LOCAL_FILESYSTEM_BACKEND_NAME))
			return new HadoopArchiveFileSystem(
					getHadoopFileSystemSafe(URI.create("file:/")));
		else if (backend.equals("hdfs"))
			return HadoopArchiveFileSystemFactory.create();
		else if (backend.equals("s3"))
			return S3ArchiveFileSystemFactory.createS3();
		else if (backend.equals("s3n"))
			return S3ArchiveFileSystemFactory.createS3n();
		else if (backend.equals("glacier"))
			return GlacierArchiveFileSystemFactory.create(localFileSystemPaths);
		else
			throw new IllegalStateException(
					"Supported URI schemas should return a ArchiveFileSystem.");
	}

	private static FileSystem getHadoopFileSystemSafe(URI uri) {
		try {
			return FileSystem.get(uri, new Configuration());
		} catch (IOException e) {
			logger.error(did("Tried to create Hadoop FileSystem with uri", e,
					"To create file system.", "uri", uri));
			throw new RuntimeException("Could not connect to hadoop", e);
		}
	}
}
