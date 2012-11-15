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
package com.splunk.shuttl.archiver.clustering;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.splunk.Service;

/**
 * Calls splunk instances shuttl port.
 */
public class ShuttlPortEntity {

	private final Service service;
	private final JsonRestEndpointCaller restEndpointCaller;

	private ShuttlPortEntity(Service service,
			JsonRestEndpointCaller jsonRestEndpointCaller) {
		this.service = service;
		this.restEndpointCaller = jsonRestEndpointCaller;
	}

	/**
	 * @return the configured shuttl port of the Splunk instance which the service
	 *         is connected to.
	 */
	public int getShuttlPort() {
		HttpGet httpGet = createHttpGetRequest();
		JSONObject jsonObject = restEndpointCaller.getJson(httpGet);
		return getShuttlPortFromJSONResponse(jsonObject);
	}

	private HttpGet createHttpGetRequest() {
		URI shuttlPortRequestUri = URI
				.create(service.getScheme() + "://" + service.getHost() + ":"
						+ service.getPort() + "/services/shuttl/port");
		HttpGet httpGet = new HttpGet(shuttlPortRequestUri);
		return httpGet;
	}

	private int getShuttlPortFromJSONResponse(JSONObject jsonObject) {
		try {
			return Integer.parseInt(jsonObject.getString("shuttl_port"));
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	public static ShuttlPortEntity create(Service splunkService) {
		return new ShuttlPortEntity(splunkService, new JsonRestEndpointCaller(
				getInsecureHttpClient()));
	}

	/**
	 * @return
	 */
	@SuppressWarnings("deprecation")
	private static HttpClient getInsecureHttpClient() {
		KeyStore trustStore = getTrustStore();
		SSLSocketFactory sf = createSSLSocketFactory(trustStore);
		sf.setHostnameVerifier(SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

		HttpParams params = new BasicHttpParams();
		HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
		HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);

		SchemeRegistry registry = new SchemeRegistry();
		registry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(),
				80));
		registry.register(new Scheme("https", sf, 443));

		ClientConnectionManager ccm = new ThreadSafeClientConnManager(params,
				registry);

		return new DefaultHttpClient(ccm, params);
	}

	private static SSLSocketFactory createSSLSocketFactory(KeyStore trustStore) {
		try {
			return new EasySSLSocketFactory(trustStore);
		} catch (KeyManagementException e) {
			throw new RuntimeException(e);
		} catch (UnrecoverableKeyException e) {
			throw new RuntimeException(e);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		} catch (KeyStoreException e) {
			throw new RuntimeException(e);
		}
	}

	private static KeyStore getTrustStore() {
		try {
			KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
			trustStore.load(null, null);
			return trustStore;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static TrustManager getTrustManager() {
		return new X509TrustManager() {
			@Override
			public void checkClientTrusted(X509Certificate[] arg0, String arg1)
					throws CertificateException {
			}

			@Override
			public void checkServerTrusted(X509Certificate[] arg0, String arg1)
					throws CertificateException {
			}

			@Override
			public X509Certificate[] getAcceptedIssuers() {
				return null;
			};
		};
	}

	private static class EasySSLSocketFactory extends SSLSocketFactory {
		SSLContext sslContext = SSLContext.getInstance("TLS");

		public EasySSLSocketFactory(KeyStore truststore)
				throws NoSuchAlgorithmException, KeyManagementException,
				KeyStoreException, UnrecoverableKeyException {
			super(truststore);
			TrustManager tm = getTrustManager();
			sslContext.init(null, new TrustManager[] { tm }, new SecureRandom());
		}

		@Override
		public Socket createSocket(Socket socket, String host, int port,
				boolean autoClose) throws IOException, UnknownHostException {
			return sslContext.getSocketFactory().createSocket(socket, host, port,
					autoClose);
		}

		@Override
		public Socket createSocket() throws IOException {
			return sslContext.getSocketFactory().createSocket();
		}

	}
}
