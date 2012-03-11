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

package com.splunk.shep.mapreduce.lib.rest.util;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * This class helps to create a https connection even when the server does not
 * have a valid certificate
 * 
 * @author kpakkirisamy
 * 
 */
public class HttpClientUtils {
    public static HttpClient getHttpClient() throws Exception {
	HttpClient base = new DefaultHttpClient();
	try {
	    SSLContext ctx = SSLContext.getInstance("TLS");
	    X509TrustManager tm = new X509TrustManager() {
		public void checkClientTrusted(X509Certificate[] xcs,
			String string) throws CertificateException {
		}

		public void checkServerTrusted(X509Certificate[] xcs,
			String string) throws CertificateException {
		}

		public X509Certificate[] getAcceptedIssuers() {
		    return null;
		}
	    };
	    ctx.init(null, new TrustManager[] { tm }, null);
	    SSLSocketFactory ssf = new SSLSocketFactory(ctx);
	    ssf.setHostnameVerifier(SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
	    ClientConnectionManager ccm = base.getConnectionManager();
	    SchemeRegistry sr = ccm.getSchemeRegistry();
	    sr.register(new Scheme("https", ssf, 443));
	    return new DefaultHttpClient(ccm, base.getParams());
	} catch (Exception ex) {
	    ex.printStackTrace();
	    throw ex;
	}
    }

}
