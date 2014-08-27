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
package com.splunk.shuttl.archiver.filesystem.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.BackendConfigurationFiles;

/**
 *
 */
public class HadoopArchiveFileSystemFactory {

	/**
	 * @return Hadoop file system as a back-end with properties in the
	 *         shuttl/conf/backend directory.
	 */
	public static ArchiveFileSystem create() {
		return createWithPropertyFile(BackendConfigurationFiles.create().getByName(
				HdfsProperties.HDFS_PROPERTIES_FILENAME));
	}

	public static HadoopArchiveFileSystem createWithPropertyFile(
			File hdfsProperties) {
		try {
			return doCreate(hdfsProperties);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static HadoopArchiveFileSystem doCreate(File hdfsProperties)
			throws IOException {
		HdfsProperties properties = HdfsProperties.create(hdfsProperties);

		FileSystem fs = FileSystem.get(properties.getConf());
		return new HadoopArchiveFileSystem(fs);
	}
}
