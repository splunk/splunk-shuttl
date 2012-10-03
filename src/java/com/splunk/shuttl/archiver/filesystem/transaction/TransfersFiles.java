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
package com.splunk.shuttl.archiver.filesystem.transaction;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * 
 */
public interface TransfersFiles extends PutsFiles, GetsFiles {

}

interface PutsFiles {
	void putFile(File src, URI temp, URI dst) throws IOException;
}

interface GetsFiles {
	void getFile(URI src, File temp, File dst) throws IOException;
}
