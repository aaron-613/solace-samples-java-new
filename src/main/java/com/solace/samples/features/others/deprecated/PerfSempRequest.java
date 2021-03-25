/**
 * PerfSempRequest.java
 * 
 * This sample illustrates how to re-use a java.net.URL object instance to 
 * perform several SEMP requests (serially) at maximum rate to an appliance over 
 * the same connection.  
 * 
 * Copyright 2009-2020 Solace Corporation. All rights reserved.
 */

package com.solace.samples.features.others.deprecated;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.Callable;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.solace.samples.features.common.SampleApp;

public class PerfSempRequest extends SampleApp {

	public class MyAuthenticator extends Authenticator {
        String user;
        String pwd;
        public MyAuthenticator(String username, String password) {
            user = username;
            pwd = password;
        }
        protected PasswordAuthentication getPasswordAuthentication() {
            return new PasswordAuthentication (user, pwd.toCharArray());
        }
    }
    
    public PerfSempRequest() {
		super();
	}
    
    public void printSyntax() {
        System.out.println("Parameters:");
        System.out.println("\t-h HOST:PORT\t\tAppliance Management IP Address");
        System.out.println("\t[-u USER]\t\tAuthentication Username");
        System.out.println("\t[-w PASSWORD]\t\tAuthentication Password");
        System.out.println("\t[-sv SEMP_VERSION]\tSEMP version in the SEMP request. Default: " + SEMP_VERSION_TR);
        System.out.println("\t[-s]\t\t\tUse HTTPS");
        System.out.println();
    }
    public void run(String[] args) {
        try {
            String ipPort = null;
            String username = "admin";
            String password = "admin";
            String sempVersion = SEMP_VERSION_TR;
            boolean useHttps = false;
            
            for(int i = 0; i < args.length; i++) {
                if (args[i].equals("-h")) {
                    i++;
                    ipPort = args[i];
                } else if (args[i].equals("-u")) {
                    i++;
                    username = args[i];
                } else if (args[i].equals("-w")) {
                    i++;
                    password = args[i];
                } else if (args[i].equals("-sv")) {
                	i++;
                	sempVersion = args[i];
                } else if (args[i].equals("-s")) {
                    useHttps = true;
                }
            }
			if (ipPort == null) {
				printSyntax();
				return;
			}
            
            // Very fast SEMP request to execute.
            final String request = "<rpc semp-version=\"" + sempVersion + "\"><show><hostname/></show></rpc>";
            
			// Default authenticator for connections. Java will first attempt an
			// unauthenticated connection, then automatically retry with auth
			// when that first connection fails.
			Authenticator.setDefault(new MyAuthenticator(username, password));

			// Setup for https.
			// Create a trust manager that accepts all certificates.
			// Create a hostname verifier that accepts all hostnames.
            String scheme = "http://";
			if (useHttps) {
			    scheme = "https://";
	            SSLContext sc = SSLContext.getInstance("TLSv1");
	            sc.init(null, new TrustManager[] {new X509TrustManager() {
	                public X509Certificate[] getAcceptedIssuers() {
	                    return null;
	                }
	                public void checkServerTrusted(X509Certificate[] chain, String authType)
	                        throws CertificateException {
	                }
	                public void checkClientTrusted(X509Certificate[] chain, String authType)
	                        throws CertificateException {
	                }
	            }}, null);
	            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
	            HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
	                public boolean verify(String arg0, SSLSession arg1) {
	                    return true;
	                 }
	            }); 
			}
			
			// We use a URL instance to get HttpURLConnections; Java's HTTP
			// handler keeps the underlying connection open and we can make
			// several requests without reconnecting by calling
			// openConnection().
			URL url = new URL(scheme + ipPort + "/SEMP");

			// ----- Warmup (connect + authenticate) -----
			System.out.println("Testing... ");
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			Callable<Integer> c = new BlockingSempRequest(connection, request, 0, true);
			System.out.println("... Response Code: " + c.call());
			// ----- End warmup (we are connected) -----
			
			// Single thread doing SEMP calls on a single connection.
			final int TEST_LENGTH = 5;
			long startTime = System.currentTimeMillis();
			long stopTime = startTime + TEST_LENGTH * 1000;
			System.out.printf("%s second test...\n", TEST_LENGTH);
			int i = 0;
			while (System.currentTimeMillis() < stopTime) {
				connection = (HttpURLConnection) url.openConnection();
				c = new BlockingSempRequest(connection, request, i, false);
				c.call(); // Perform request
				i++;
				printDot();
			}
			
			System.out.printf("\nSingle thread requestor ran %s requests in %s seconds.\n", i, TEST_LENGTH);

        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
	private static int dotsPrinted = 0;

	public synchronized static void printDot() {
		System.out.print(".");
		if (++dotsPrinted % 75 == 0) System.out.println();
	}
	
	public static class BlockingSempRequest implements Callable<Integer> {
		final HttpURLConnection connection;
		final String request;
		final int _id;
		final boolean _printResponse;

		public BlockingSempRequest(HttpURLConnection conn, String req, int id, boolean printResponse) {
			connection = conn;
			request = req;
			_id = id;
			_printResponse = printResponse;
		}

		public Integer call() throws Exception {
			connection.setRequestMethod("POST");
			connection.setDoInput(true);
			connection.setDoOutput(true);
            
			OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
			out.write(request);
			out.close();

            int responseCode = connection.getResponseCode();
			if (responseCode == 200) {
				BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
				String inputLine;
				while ((inputLine = in.readLine()) != null) {
					// read / print response
					if (_printResponse) System.out.println(inputLine);
				}
				in.close();
            } else {
				System.out.printf("\n[%s] Error: %s %s.\n", _id, responseCode, connection.getResponseMessage());
				throw new Exception(String.format("Response code not 200, was %s.", responseCode));
            }            
			
			return Integer.valueOf(responseCode);
		}
	}
    
    public static void main(String[] args) {
        PerfSempRequest httpSempReq = new PerfSempRequest();
        httpSempReq.run(args);
        System.exit(0);
    }
    
}
