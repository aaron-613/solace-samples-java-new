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
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

/**
 * A Processor is a microservice/application that receives a message, does something with the info,
 * and then sends it on..!
 */
public class DirectProcessor {

    private static final String TOPIC_PREFIX = "solace/samples";  // used as the topic "root"

    public void run(String... args) throws JCSMPException {
        System.out.println(DirectProcessor.class.getSimpleName()+" initializing...");
        
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

        /** Simple anonymous inner-class for handling publishing events */
        final XMLMessageProducer producer = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
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

                }
            }
        });

        /** Simple anonymous inner-class for request handling **/
        final XMLMessageConsumer cons = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage inboundMsg) {
                if (inboundMsg.getReplyTo() == null) {  // not expecting reply
                    TextMessage outboundMsg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
                    // how to process the incoming message? maybe do a DB lookup? or add some payload
                    final String upperCaseMessage = inboundMsg.dump().toUpperCase();  // as an example of "processing"
                    outboundMsg.setText(upperCaseMessage);
                    String onwardsTopic = new StringBuilder(TOPIC_PREFIX).append("/direct/proc/").append(inboundMsg.getDestination().getName()).toString();//.charAt(index)toUpperCase();
                    try {
                        producer.send(outboundMsg, JCSMPFactory.onlyInstance().createTopic(onwardsTopic));
                    } catch (JCSMPException e) {
                        System.out.println("### Error sending reply.");
                        e.printStackTrace();

                    }
                } else {
                    System.out.println("Received message without reply-to field");
                }
            }

            public void onException(JCSMPException e) {
                System.out.printf("Consumer received exception: %s%n", e);
            }
        });

        session.connect();
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX+"/direct/pub/>"));  // listen to
        cons.start();

        // Consume-only session is now hooked up and running!
        System.out.println("Listening for incoming messages. Press [ENTER] to exit");
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Close consumer
        cons.close();
        System.out.println("Exiting.");
        session.closeSession();

    }

    public static void main(String... args) throws JCSMPException {
        // Check command line arguments
        if (args.length < 3) {
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [client-password]%n%n",
                    DirectProcessor.class.getSimpleName());
            System.exit(-1);
        }
        DirectProcessor processor = new DirectProcessor();
        processor.run(args);
    }
}
