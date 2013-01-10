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

import java.util.List;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class JsonUtils {

	/**
	 * Merge a key in Json objects. Recommend viewing tests to see how the merging
	 * works.
	 */
	public static JSONObject mergeKey(List<JSONObject> jsons, String key) {
		try {
			return doMergeKey(jsons, key);
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	private static JSONObject doMergeKey(List<JSONObject> jsons, String key)
			throws JSONException {
		JSONObject merged = new JSONObject();
		for (JSONObject json : jsons)
			mergeJsonAtKey(merged, json, key);
		return merged;
	}

	private static void mergeJsonAtKey(JSONObject merged, JSONObject json,
			String key) throws JSONException {
		Object value = getJsonKeyOrNull(json, key);
		if (value != null) {
			if (value instanceof JSONArray) {
				mergeJsonArray(merged, key, (JSONArray) value);
			} else {
				appendKeyValue(merged, key, value);
			}
		}
	}

	private static Object getJsonKeyOrNull(JSONObject json, String key)
			throws JSONException {
		try {
			return json.get(key);
		} catch (JSONException e) {
			return null;
		}
	}

	private static void mergeJsonArray(JSONObject merged, String key,
			JSONArray array) throws JSONException {
		for (int i = 0; i < array.length(); i++)
			appendKeyValue(merged, key, array.get(i));
	}

	private static void appendKeyValue(JSONObject merged, String key, Object value)
			throws JSONException {
		merged.accumulate(key, value);
	}

}
