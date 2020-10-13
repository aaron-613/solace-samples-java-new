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

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class DirectPublisher {
    
    private static final String TOPIC_PREFIX = "solace/samples";  // used as the topic "root"
    private static final int APPROX_MSG_RATE_PER_SEC = 10;
    private static final int PAYLOAD_SIZE = 100;
    private static volatile boolean isShutdownFlag = false;

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
        channelProps.setReconnectRetries(20);      // recommended settings
        channelProps.setConnectRetriesPerHost(5);  // recommended settings
        // https://docs.solace.com/Solace-PubSub-Messaging-APIs/API-Developer-Guide/Configuring-Connection-T.htm
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES,channelProps);
        final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
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
                    isShutdownFlag = true;
                }
            }
        });
        // done boilerplate
        
        Runnable pubThread = () -> {  // create an application thread for publishing in a loop
            String topicString;
            BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);  // use a binary message
            byte[] payload = new byte[PAYLOAD_SIZE];
            char characterOfTheMoment;  // to have variable non-trivial test payloads
            try {
                while (!isShutdownFlag) {
                    // each loop, make a new payload to send
                    characterOfTheMoment = (char)(Math.round(System.nanoTime()%26)+65);  // choose a "random" letter [A-Z]
                    Arrays.fill(payload,(byte)characterOfTheMoment);  // fill the payload completely with that char
                    message.setData(payload);
                    // dynamic topics!! use StringBuilder because "+" concat operator is SLOW
                    topicString = new StringBuilder(TOPIC_PREFIX)
                            .append("/direct/")
                            .append(characterOfTheMoment)
                            .toString();
                    producer.send(message,JCSMPFactory.onlyInstance().createTopic(topicString));  // send the message
                    message.reset();  // reuse this message, to avoid having to recreate it: better performance
                    try {
                        //Thread.sleep(1000);  // set to 0 for max speed
                        Thread.sleep(1000/APPROX_MSG_RATE_PER_SEC);  // do Thread.sleep(0) for max speed
                        // Note: STANDARD Edition PubSub+ broker is limited to 10k max ingress
                    } catch (InterruptedException e) {
                        isShutdownFlag = true;
                    }
                }
            } catch (JCSMPException e) {
                e.printStackTrace();
            } finally {
                System.out.print("Shutdown! Stopping Publisher... ");
                producer.close();
                session.closeSession();
                System.out.println("Done.");
            }
        };
        Thread t = new Thread(pubThread,"Publisher Thread");
        t.setDaemon(true);
        t.start();

        System.out.println("Connected, and running. Press [ENTER] to quit.");
        // block the main thread, waiting for a quit signal
        System.in.read();  // wait for user to end
        System.out.println("ENTER pressed. Quitting in 1 second.");
        // send a quit control message to anyone listening
        TextMessage quitMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        producer.send(quitMessage,JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX+"/quit"));
        isShutdownFlag = true;
        Thread.sleep(1000);
    }
}
