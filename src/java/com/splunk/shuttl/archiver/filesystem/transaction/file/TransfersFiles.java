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
package com.splunk.shuttl.archiver.filesystem.transaction.file;

import java.io.File;
import java.net.URI;

import com.splunk.shuttl.archiver.filesystem.transaction.TransfersData;

/**
 * Extends {@link TransfersData} with String, and represents Files as String
 * paths to files. Reason being that remote files should not be represented by
 * {@link File}, and we don't want to be dependent on {@link URI}, because where
 * the path is transfered to is up to the back end implementation.
 */
public interface TransfersFiles extends TransfersData<String> {

}
