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
package com.splunk.shuttl.archiver.fileSystem;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;

/**
 * Factory for getting an {@link ArchiveFileSystem}
 */
public class ArchiveFileSystemFactory {

    private static final Logger logger = Logger
	    .getLogger(ArchiveFileSystemFactory.class);
    private static final Set<String> supportedUriSchemas;

    static {
	supportedUriSchemas = new HashSet<String>();
	supportedUriSchemas.add("file");
	supportedUriSchemas.add("hdfs");
    }

    /**
     * @return true if {@link ArchiveFileSystemFactory} can create an
     *         {@link ArchiveFileSystem} from that URI.
     */
    public static boolean isSupportedUri(URI create) {
	return supportedUriSchemas.contains(create.getScheme());
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
	return getForUriToTmpDir(config.getTmpDirectory());
    }

    /**
     * Creates a {@link ArchiveFileSystem} for a URI to tmp path.</br> Example:
     * 'file:/tmp' contains the scheme and the path to a tmp directory.</br>
     * 'hdfs://localhost:1234/archive-tmp' contains scheme, host, port and path
     * to directory.
     * 
     * @return
     */
    public static ArchiveFileSystem getForUriToTmpDir(URI uri) {
	if (!supportedUriSchemas.contains(uri.getScheme())) {
	    throw new UnsupportedUriException("Supported Uri schemas are: "
		    + supportedUriSchemas);
	} else {
	    return supportedArchiveFileSystem(uri);
	}
    }

    private static ArchiveFileSystem supportedArchiveFileSystem(URI uri) {
	if (uri.getScheme().equals("file") || uri.getScheme().equals("hdfs")) {
	    return createHadoopFileSystem(uri);
	}

	throw new IllegalStateException(
		"Supported URI schemas should return a ArchiveFileSystem.");
    }

    private static ArchiveFileSystem createHadoopFileSystem(URI uri) {
	FileSystem hadoopFs = getHadoopFileSystemSafe(uri);
	return new HadoopFileSystemArchive(hadoopFs, new Path(uri.getPath()));
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
