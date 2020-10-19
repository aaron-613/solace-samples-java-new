/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.solace.samples;

import java.io.IOException;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPRequestTimeoutException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.Requestor;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

/** Direct Messaging sample to demonstrate initiating a request-reply flow.
 *  Makes use of the blocking convenience function Requestor.request(
 */
public class DirectRequestor {
    
    private static final String SAMPLE_NAME = DirectRequestor.class.getSimpleName();
    private static final String TOPIC_PREFIX = "solace/samples";  // used as the topic "root"
    private static final int APPROX_MSG_RATE_PER_SEC = 1;

    private static volatile boolean isShutdown = false;


    public static void main(String... args) throws JCSMPException, IOException {
        if (args.length < 3) {  // Check command line arguments
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [client-password]%n%n",
                    SAMPLE_NAME);
            System.exit(-1);
        }
        System.out.println(SAMPLE_NAME+" initializing...");

        // Create a JCSMP Session
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);          // host:port
        properties.setProperty(JCSMPProperties.VPN_NAME,  args[1]);     // message-vpn
        properties.setProperty(JCSMPProperties.USERNAME, args[2]);      // client-username
        if (args.length > 3) {
            properties.setProperty(JCSMPProperties.PASSWORD, args[3]);  // client-password
        }
        properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);  // re-subscribe after reconnect
        JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
        channelProps.setReconnectRetries(20);      // recommended settings
        channelProps.setConnectRetriesPerHost(5);  // recommended settings
        // https://docs.solace.com/Solace-PubSub-Messaging-APIs/API-Developer-Guide/Configuring-Connection-T.htm
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES,channelProps);
        final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();

        /** Anonymous inner-class for handling publishing events */
        @SuppressWarnings("unused")
        XMLMessageProducer producer = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
            @Override @SuppressWarnings("deprecation")
            public void responseReceived(String messageID) {
                // deprecated, superseded by responseReceivedEx()
            }
            @Override @SuppressWarnings("deprecation")
            public void handleError(String messageID, JCSMPException e, long timestamp) {
                // deprecated, superseded by handleErrorEx()
            }
            @Override public void responseReceivedEx(Object key) {
                // unused in Direct Messaging application, only for Guaranteed/Persistent publishing application
            }
            // can be called for ACL violations, connection loss, and Persistent NACKs
            @Override
            public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
                System.out.printf("### Producer handleErrorEx() callback: %s%n",cause);
                if (cause instanceof JCSMPTransportException) {  // unrecoverable
                    isShutdown = true;
                }
            }
        });

//        XMLMessageConsumer consumer = session.getMessageConsumer((XMLMessageListener)null);  // rare use of synchronous consumer mode
        XMLMessageConsumer consumer = session.getMessageConsumer(new XMLMessageListener() {
            
            @Override
            public void onReceive(BytesXMLMessage message) {
                System.out.println("Received a message!");
            }
            
            @Override
            public void onException(JCSMPException e) {
                System.out.printf("### MessageListener's onException(): %s%n",e);
                if (e instanceof JCSMPTransportException) {  // unrecoverable
                    isShutdown = true;
                }
            }
        });
        consumer.start();  // needed to accept the responses

        //Time to wait for a reply before timing out
        final int timeoutMs = 10000;
        TextMessage requestMsg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        final String text = "Sample Request";
        //request.setText(text);

        try {
            while (System.in.available() == 0 && !isShutdown) {
                try {
                    Requestor requestor = session.createRequestor();  // create the Requestor object
                    Topic topic = JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX+"/direct/request");
                    System.out.printf("About to send request message '%s' to topic '%s'...%n",text,topic.getName());
                    // perform blocking/synchronous call using convenience method request()
                    BytesXMLMessage reply = requestor.request(requestMsg, timeoutMs, topic);
                    System.out.printf("Response Message Dump:%n%s%n",reply.dump());
                    Thread.sleep(APPROX_MSG_RATE_PER_SEC/1000);
                } catch (JCSMPRequestTimeoutException e) {
                    System.out.println("Failed to receive a reply in " + timeoutMs + " msecs");
                } catch (InterruptedException e1) {
                    isShutdown = true;
                }
            }
        } finally {
            System.out.println("Exiting...");
            session.closeSession();
        }
    }
}
