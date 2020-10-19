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
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class DirectReplier {

    private static final String SAMPLE_NAME = DirectReplier.class.getSimpleName();
    private static final String TOPIC_PREFIX = "solace/samples";  // used as the topic "root"

    private static volatile boolean isShutdown = false;

    public static void main(String... args) throws JCSMPException {
        // Check command line arguments
        if (args.length < 3) {
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [client-password]%n%n",
                    SAMPLE_NAME);
            System.exit(-1);
        }

        System.out.println(SAMPLE_NAME+" initializing...");
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
        final XMLMessageProducer producer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
            @Override
            public void responseReceived(String messageID) {
                System.out.println("Producer received response for msg: " + messageID);
            }

            @Override
            public void handleError(String messageID, JCSMPException e, long timestamp) {
                System.out.printf("Producer received error for msg: %s@%s - %s%n", messageID, timestamp, e);
            }
        });

        /** Anonymous inner-class for request handling **/
        final XMLMessageConsumer cons = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage requestMsg) {
                // we will reply from within the callback, only allows for Direct messages
                
                if (requestMsg.getDestination().getName().contains("direct/request") && requestMsg.getReplyTo() != null) {
                    System.out.printf("Received request on '%s', generating response.",requestMsg.getDestination());
                    TextMessage replyMsg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);  // reply with a Text
                    if (requestMsg.getApplicationMessageId() != null) {
                        replyMsg.setApplicationMessageId(requestMsg.getApplicationMessageId());  // populate for traceability
                    }
                    final String text = "Hello! Here is a response to your message on topic '"+requestMsg.getDestination()+"'.";
                    replyMsg.setText(text);
                    try {
                        producer.sendReply(requestMsg, replyMsg);  // convenience method: copies in reply-to, correlationId, etc.
                    } catch (JCSMPException e) {
                        System.out.println("Error sending reply.");
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

        // topic to listen to incoming (messaging) requests
        final Topic topic = JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX+"/direct/request\u0003");
        // for use with HTTP MicroGateway feature, will respond to GET request on same URI
        final Topic topic2 = JCSMPFactory.onlyInstance().createTopic("GET/"+TOPIC_PREFIX+"/direct/request/\u0003");
        
        session.addSubscription(topic);
        session.addSubscription(topic2);
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX+"/control/>"));  // add multiple wildcard subscriptions
        cons.start();

        // Consume-only session is now hooked up and running!
        System.out.println("Listening for request messages on topic " + topic + " ... Press enter to exit");
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
}
