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
import java.util.Arrays;
import java.util.UUID;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class DirectPublisher {
    
    private static final String TOPIC_PREFIX = "solace/samples";  // used as the topic "root"
    private static final int APPROX_MSG_RATE_PER_SEC = 2;
    private static final int PAYLOAD_SIZE = 100;
    private static volatile int msgSentCounter = 0;                   // num messages sent
    private static volatile boolean isShutdown = false;

    public static void main(String... args) throws JCSMPException, IOException, InterruptedException {
        // Check command line arguments
        if (args.length < 3) {
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [client-password]%n%n",
                    DirectPublisher.class.getSimpleName());
            System.exit(-1);
        }
        System.out.println(DirectPublisher.class.getSimpleName()+" initializing...");

        // Create a JCSMP Session
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);          // host:port
        properties.setProperty(JCSMPProperties.VPN_NAME,  args[1]);     // message-vpn
        properties.setProperty(JCSMPProperties.USERNAME, args[2]);      // client-username
        if (args.length > 3) { 
            properties.setProperty(JCSMPProperties.PASSWORD, args[3]);  // client-password
        }
        properties.setProperty(JCSMPProperties.GENERATE_SEQUENCE_NUMBERS,true);  // why not?
        JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
//        channelProps.setReconnectRetries(20);      // recommended settings
//        channelProps.setConnectRetriesPerHost(5);  // recommended settings
        // https://docs.solace.com/Solace-PubSub-Messaging-APIs/API-Developer-Guide/Configuring-Connection-T.htm
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES,channelProps);
        final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties,null,new SessionEventHandler() {
            @Override
            public void handleEvent(SessionEventArgs event) {
                System.out.printf("### SessionEventHandler handleEvent() callback: %s%n",event);
            }
        });
        session.connect();

        /** Anonymous inner-class for handling publishing events */
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
                } else if (cause instanceof JCSMPErrorResponseException) {  // might have some extra info
                    JCSMPErrorResponseException e = (JCSMPErrorResponseException)cause;
                    System.out.println(JCSMPErrorResponseSubcodeEx.getSubcodeAsString(e.getSubcodeEx())+": "+e.getResponsePhrase());
                }
            }
        },null);  // null is the ProducerEvent handler... do not need it in this simple application 
        // done boilerplate
        
        Runnable pubThread = () -> {  // create an application thread for publishing in a loop
            String topicString;
            BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);  // preallocate a binary message
            byte[] payload = new byte[PAYLOAD_SIZE];  // preallocate, for reuse
            while (!isShutdown) {
                try {
                    // each loop, make a new payload to send
                    char chosenCharacter = (char)(Math.round(System.nanoTime()%26)+65);  // choose a "random" letter [A-Z]
                    Arrays.fill(payload,(byte)chosenCharacter);  // fill the payload completely with that char
                    message.setData(payload);
                    // dynamic topics!! use StringBuilder because "+" concat operator is SLOW
                    topicString = new StringBuilder(TOPIC_PREFIX).append("/direct/pub/").append(chosenCharacter).toString();
                    message.setApplicationMessageId(UUID.randomUUID().toString());  // as an example
                    producer.send(message,JCSMPFactory.onlyInstance().createTopic(topicString));  // send the message
                    msgSentCounter++;  // add one
                    message.reset();  // reuse this message, to avoid having to recreate it: better performance
                    try {
//                        Thread.sleep(0);
                        Thread.sleep(1000/APPROX_MSG_RATE_PER_SEC);  // do Thread.sleep(0) for max speed
                        // Note: STANDARD Edition Solace PubSub+ broker is limited to 10k msg/s max ingress
                    } catch (InterruptedException e) {
                        isShutdown = true;
                    }
                } catch (JCSMPException e) {  // threw from send(), only thing that is throwing here, but keep trying (unless shutdown?)
                    System.out.printf("### Caught while trying to producer.send(): %s%n",e);
                    if (e instanceof JCSMPTransportException) {  // unrecoverable
                        isShutdown = true;
                    }
                }
            }
        };
        Thread t = new Thread(pubThread,"Publisher Thread");
        t.setDaemon(true);
        t.start();

        System.out.println("Connected, and running. Press [ENTER] to quit.");
        // block the main thread, waiting for a quit signal
        while (System.in.available() == 0 && !isShutdown) {
            Thread.sleep(1000);
            System.out.printf("Published msgs/s: %,d%n",msgSentCounter);  // simple way of calculating message rates
            msgSentCounter = 0;
        }
        System.out.println("Quitting in 1 second.");
        isShutdown = true;
        Thread.sleep(1000);
        session.closeSession();  // will also close producer and consumer objects
    }
}
