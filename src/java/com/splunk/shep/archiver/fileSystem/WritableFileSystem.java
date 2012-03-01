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
package com.splunk.shep.archiver.fileSystem;

import java.net.URI;

import org.apache.hadoop.fs.FileSystem;

/**
 * Provides a writable path on a file system, including the file systems schema
 * and authority.<br/>
 * See {@link WritableFileSystem#getWritableUri()} for examples.
 */
public interface WritableFileSystem {

    /**
     * Get a writable path on a file system in the form of a {@link URI}. The
     * {@link URI} also contains the file systems schema and authority, much
     * like {@link FileSystem#getUri()} <br/>
     * <br/>
     * Schema and authority: A local file system would have the schema and
     * authority "file://" and an HDFS file system would have "hdfs://host:port"<br/>
     * Writable URI example: A local file system's user's home directory is
     * writable, so this method could for a local file system a URI that equals
     * "file:///Users/username"
     * 
     * @return {@link URI} whose scheme and authority identify this FileSystem,
     *         and the {@link URI}'s path is writable.
     */
    public URI getWritableUri();

}
