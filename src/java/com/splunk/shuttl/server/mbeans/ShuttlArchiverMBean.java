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
package com.splunk.shuttl.server.mbeans;

import java.util.List;

/**
 * @author kpakkirisamy
 * 
 */
public interface ShuttlArchiverMBean {

	public static final String OBJECT_NAME = "com.splunk.shuttl.mbeans:type=Archiver";

	public String getLocalArchiverDir();

	public void setLocalArchiverDir(String localArchiverDir);

	public List<String> getArchiveFormats();

	public void setArchiveFormats(List<String> archiveFormats);

	public String getClusterName();

	public void setClusterName(String clusterName);

	public List<String> getBucketFormatPriority();

	public void setBucketFormatPriority(List<String> priorityList);

	public String getServerName();

	public void setServerName(String serverName);

	public String getBackendName();

	public void setBackendName(String backendName);

	public String getArchivePath();

	public void setArchivePath(String archivePath);

}
