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
package com.splunk;

import java.net.URI;

/**
 * Extension of the Splunk SDK for the Cluster Config endpoint. <br/>
 * 
 * Used for getting a Splunk cluster slave's cluster master uri.
 */
public class ClusterConfig extends Entity {

	public ClusterConfig(Service service) {
		super(service, "cluster/config/config");
	}

	public URI getClusterMasterUri() {
		return URI.create(getString("master_uri", null));
	}
}
