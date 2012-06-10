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
package com.splunk.shuttl.archiver.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Utils for {@link List}
 */
public class UtilsList {

	/**
	 * @param list1
	 * @param list2
	 * @return a list containing list1 and list2
	 */
	public static List<String> join(List<String> list1, List<String> list2) {
		List<String> join = new ArrayList<String>(list1);
		join.addAll(list2);
		return join;
	}

}
