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
package com.splunk.shep;

/**
 * @author hyan
 *
 */
public class ShepConstants {
    public static final String ENDPOINT_CONTEXT = "shep/rest";
    public static final String ENDPOINT_DEFAULT_HOST = "/defaulthost";
    public static final String ENDPOINT_DEFAULT_PORT = "/defaultport";
    public static final String ENDPOINT_SERVER = "/server";
    public static final String ENDPOINT_FORWARDER = "/forwarder";
    public static final String ENDPOINT_SINK_PREFIX = "/sinkprefix";
    public static final String ENDPOINT_BUCKET_ARCHIVER = "/bucket/archive";
    public static final String ENDPOINT_ARCHIVER = "/archiver";
    public static final String ENDPOINT_SHUTDOWN = "/shutdown";
    public static final String ENDPOINT_EXPORT_SRVC_STATUS = "/exportservicestatus";
    public static final String ATT_DEF_HADOOP_CLUSTER_HOST = "DefHadoopClusterHost";
    public static final String ATT_DEF_HADOOP_CLUSTER_PORT = "DefHadoopClusterPort";
}
