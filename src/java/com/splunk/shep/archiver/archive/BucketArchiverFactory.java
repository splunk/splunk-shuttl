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
package com.splunk.shep.archiver.archive;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;

import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.fileSystem.HadoopFileSystemArchive;
import com.splunk.shep.archiver.fileSystem.WritableFileSystem;

/**
 * Construction code for creating BucketArchivers that archives in different
 * FileSystems.
 */
public class BucketArchiverFactory {

    /**
     * Creates the currently default archiver.
     */
    public static BucketArchiver createDefaultArchiver() {
	return createHdfsArchiver();
    }

    /**
     * Create {@link BucketArchiver} that uses Hadoop's HDFS file system for
     * archiving buckets. <br/>
     * CONFIG: The host and port of the HDFS used is currently hard coded.
     * Should be configurable.
     */
    public static BucketArchiver createHdfsArchiver() {
	return createHadoopFileSystemArchiver(getHdfsFileSystem());
    }

    /**
     * Create {@link BucketArchiver} that uses Hadoop's {@link LocalFileSystem}
     * for archiving buckets.
     */
    public static BucketArchiver createHadoopLocalFileSystemArchiver() {
	return createHadoopFileSystemArchiver(getHadoopLocalFileSystem());
    }

    private static BucketArchiver createHadoopFileSystemArchiver(
	    FileSystem hadoopFileSystem) {
	ArchiveConfiguration config = ArchiveConfiguration.getSharedInstance();
	ArchiveFileSystem archiveFileSystem = new HadoopFileSystemArchive(
		hadoopFileSystem, config.getTmpDirectory());

	return new BucketArchiver(config, new BucketExporter(),
		getPathResolver(hadoopFileSystem, config),
		new ArchiveBucketTransferer(archiveFileSystem));
    }

    /**
     * @return
     */
    private static FileSystem getHadoopLocalFileSystem() {
	try {
	    return FileSystem.getLocal(new Configuration());
	} catch (IOException e) {
	    // LOG
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    private static FileSystem getHdfsFileSystem() {
	try {
	    return FileSystem.get(ArchiveConfiguration.getSharedInstance()
		    .getArchiverHadoopURI(), new Configuration());
	} catch (IOException e) {
	    // LOG
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    private static PathResolver getPathResolver(FileSystem hadoopFileSystem,
	    ArchiveConfiguration config) {
	PathResolver pathResolver = new PathResolver(config,
		new HadoopWritableFileSystemUri(hadoopFileSystem));
	return pathResolver;
    }

    private static class HadoopWritableFileSystemUri implements
	    WritableFileSystem {

	private final FileSystem fileSystem;

	public HadoopWritableFileSystemUri(FileSystem fileSystem) {
	    this.fileSystem = fileSystem;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.splunk.shep.archiver.fileSystem.WritableFileSystemUri#
	 * getWritableUriOnFileSystem()
	 */
	@Override
	public URI getWritableUri() {
	    return fileSystem.getHomeDirectory().toUri();
	}
    }

}
