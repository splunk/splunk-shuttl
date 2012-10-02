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

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.splunk.shuttl.archiver.filesystem.transaction.HasFileStructure;

/**
 * Since HadoopHasDirectories, it can make them and rename them.
 */
public class HadoopFileStructure implements HasFileStructure {

	private FileSystem fileSystem;

	/**
	 * @param fileSystem
	 */
	public HadoopFileStructure(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	@Override
	public void mkdirs(URI uri) throws IOException {
		mkdirsWithPath(new Path(uri));
	}

	private void mkdirsWithPath(Path path) throws IOException {
		fileSystem.mkdirs(path);
	}

	@Override
	public void rename(URI from, URI to) throws IOException {
		mkdirsWithPath(new Path(to).getParent());
		fileSystem.rename(new Path(from), new Path(to));
	}

}
