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

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPRequestTimeoutException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.Requestor;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import java.io.IOException;

/** Direct Messaging sample to demonstrate initiating a request-reply flow.
 *  Makes use of the blocking convenience function Requestor.request();
 */
public class DirectRequestor {
    
    private static final String SAMPLE_NAME = DirectRequestor.class.getSimpleName();
    private static final String TOPIC_PREFIX = "solace/samples";  // used as the topic "root"
    private static final int REQUEST_TIMEOUT_MS = 5000;  // time to wait for a reply before timing out

    private static volatile int msgSentCounter = 0;                   // num messages sent
    private static volatile boolean isShutdown = false;

    /** Main method. */
    public static void main(String... args) throws JCSMPException, IOException {
        if (args.length < 3) {  // Check command line arguments
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [password]%n%n", SAMPLE_NAME);
            System.exit(-1);
        }
        System.out.println(SAMPLE_NAME + " initializing...");

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
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, channelProps);
        final JCSMPSession session;
        session = JCSMPFactory.onlyInstance().createSession(properties, null, new SessionEventHandler() {
            @Override
            public void handleEvent(SessionEventArgs event) {  // could be reconnecting, connection lost, etc.
                System.out.printf("### Received a Session event: %s%n", event);
            }
        });
        session.connect();

        // Anonymous inner-class for handling publishing events
        session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
            @Override public void responseReceivedEx(Object key) {
                // unused in Direct Messaging application, only for Guaranteed/Persistent publishing application
            }

            // can be called for ACL violations, connection loss, and Persistent NACKs
            @Override
            public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
                System.out.printf("### Producer handleErrorEx() callback: %s%n", cause);
                if (cause instanceof JCSMPTransportException) {  // unrecoverable
                    isShutdown = true;
                }
            }
        });
        
        // less common use of SYNChronous consumer mode (null callback)
        final XMLMessageConsumer consumer = session.getMessageConsumer((XMLMessageListener)null);
        consumer.start();  // needed to accept the responses

        System.out.println(SAMPLE_NAME + " connected, and running. Press [ENTER] to quit.");
        TextMessage requestMsg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        try {
            while (System.in.available() == 0 && !isShutdown) {
                try {
                    requestMsg.setText(String.format("Hello, this is reqeust #%d", msgSentCounter));
                    Topic topic = JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX + "/direct/request");
                    System.out.printf("About to send request message #%d to topic '%s'...%n", msgSentCounter, topic.getName());
                    Requestor requestor = session.createRequestor();  // create the useful Requestor object
                    /* perform blocking/synchronous call using convenience Requestor object.   (only for Direct messaging)
                     * request() automatically populates the message's replyTo and correlationID fields:
                     *  - replyTo is a header with a topic, and determines where the Replier sends the response message
                     *  - correlationID is another header, and is used to determine _which_ outbound request this reply is for
                     */
                    requestMsg.setDeliveryMode(DeliveryMode.PERSISTENT);
                    BytesXMLMessage replyMsg = requestor.request(requestMsg, REQUEST_TIMEOUT_MS, topic);  // send and receive in one call
                    msgSentCounter++;  // add one
                    System.out.printf("Response Message Dump:%n%s%n", replyMsg.dump());
                    requestMsg.reset();  // reuse this message, to avoid having to recreate it: better performance
                    Thread.sleep(1000);  // approx. one request per second
                } catch (JCSMPRequestTimeoutException e) {
                    System.out.println("Failed to receive a reply in " + REQUEST_TIMEOUT_MS + " msecs");
                } catch (JCSMPException e) {  // threw from send(), only thing that is throwing here, but keep trying (unless shutdown?)
                    System.out.printf("### Caught while trying to producer.send(): %s%n", e);
                    if (e instanceof JCSMPTransportException) {  // unrecoverable
                        isShutdown = true;
                    }
                } catch (InterruptedException e1) {
                    // Thread.sleep interrupted... probably getting shut down
                }
            }
        } finally {
            System.out.println("Main thread quitting.");
            isShutdown = true;
            session.closeSession();  // will also close producer and consumer objects
        }
    }
}
