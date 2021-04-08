/**
 * SempHttpSetRequest.java
 * 
 * Demonstrates SEMP request over HTTP port 80.
 * 
 * Note that for portability reasons, this sample uses Java's 
 * HttpURLConnection, which has performance limitations. For better 
 * performance, application developers can use a more flexible HTTP 
 * library, such as the Apache HTTP client.
 * 
 * Copyright 2009-2020 Solace Corporation. All rights reserved.
 */

package com.solace.samples.features.others.deprecated;

import com.solace.samples.samplers.common.SampleApp;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class SempHttpSetRequest extends SampleApp {
    
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
    
    public SempHttpSetRequest() {        
        super();
    }
    
    public void printSyntax() {
        System.out.println("Parameters:");
        System.out.println("\t-h HOST:PORT\t\tAppliance Management IP Address");
        System.out.println("\t[-u USER]\t\tAuthentication Username");
        System.out.println("\t[-w PASSWORD]\t\tAuthentication Password");
        System.out.println("\t[-sv SEMP_VERSION]\tSEMP version in the SEMP request. Default: " + SEMP_VERSION_TR);
        System.out.println("\t[-s]\t\t\tUse HTTPS");
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
                System.out.println("\nError: Appliance IP Address must be specified");
                return;
            }
            
            final String request =  "<rpc semp-version=\"" + sempVersion + "\">\n" +
                    "    <show>\n" +
                    "        <stats>\n" +
                    "            <client/>\n" +
                    "        </stats>\n" +
                    "    </show>\n" +
                    "</rpc>\n";;
            
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

            URL url = new URL(scheme + ipPort + "/SEMP");
            HttpURLConnection connection = (HttpURLConnection)url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoInput(true);
            connection.setDoOutput(true);
            
            OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
            System.out.println("Sending SEMP Request to " + url + "\n\n" + request);
            out.write(request);
            out.close();

            int responseCode = connection.getResponseCode();
            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(
                                connection.getInputStream()));
                System.out.println("Received SEMP Response:\n");
                String inputLine;
                while ((inputLine = in.readLine()) != null) 
                    System.out.println(inputLine);
                in.close();
            } else {
                System.out.println("Error: " + responseCode + " " + connection.getResponseMessage());
            }            
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        SempHttpSetRequest httpSempReq = new SempHttpSetRequest();
        httpSempReq.run(args);
        System.exit(0);
    }
}
