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
import java.util.Scanner;

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
 * This simple introductory sample shows an application that both publishes and subscribes.
 */
public class DirectHelloWorldPubSub {
    
    private static final String TOPIC_PREFIX = "solace/samples";  // used as the topic "root"
    private static volatile boolean isShutdown = false;      // are we done yet?

    public static void main(String... args) throws JCSMPException, IOException, InterruptedException {
        if (args.length < 3) {  // Check command line arguments
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [client-password]%n%n",
                    DirectHelloWorldPubSub.class.getSimpleName());
            System.exit(-1);
        }
        // Build the properties object for initializing the Session
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);
        properties.setProperty(JCSMPProperties.VPN_NAME,  args[1]);
        properties.setProperty(JCSMPProperties.USERNAME, args[2]);
        if (args.length > 3) {
            properties.setProperty(JCSMPProperties.PASSWORD, args[3]);
        }
        properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);  // re-subscribe Direct subs after reconnect
        JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
        channelProps.setReconnectRetries(10);  // give more time to reconnect
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES,channelProps);
        final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
        System.out.print("Connecting... ");
        session.connect();  // connect to the broker
        System.out.println("Connected.");
        
        // setup Producer callbacks config: simple anonymous inner-class for handling publishing events
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
        }, null);  // null is the ProducerEvent handler... do not need it in this simple application

        // setup Consumer callbacks next: anonymous inner-class for Listener async threaded callbacks
        final XMLMessageConsumer consumer = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage msg) {
                // could be 4 different message types: 3 SMF ones (Text, Map, Stream) and just plain binary
                System.out.printf("vvv RECEIVED A MESSAGE vvv%n%s%n",msg.dump());  // just print
                if (msg.getDestination().getName().contains("quit")) {  // special sample message
                    System.out.println("QUIT message received, shutting down.");
                    isShutdown = true;
                }
            }

            @Override
            public void onException(JCSMPException e) {  // oh no!
                System.out.printf("### MessageListener's onException(): %s%n",e);
                if (e instanceof JCSMPTransportException) {  // unrecoverable
                    isShutdown = true;
                }
            }
        });
        
        // User prompt, to use for specific topic
        System.out.print("Enter your name, or a unique word: ");
        Scanner userInputScanner = new Scanner(System.in);
        String uniqueName = userInputScanner.next().trim().replaceAll("\\s+", "_");  // clean up whitespace
        userInputScanner.close();
        
        // Ready to start the application
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX+"/hello/>"));  // use wildcards
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX+"/control/>"));  // use wildcards
        consumer.start();  // turn on the subs, and start receiving data
        System.out.println("Connected and subscribed. Ready to publish.");

        int msgSeqNum = 0;
        TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        while (!isShutdown) {  // time to loop!
            try {
                msgSeqNum++;
            	// specify a text payload
                message.setText(String.format("Hello World #%d from %s!", msgSeqNum,uniqueName));
                // make a dynamic topic: solace/samples/hello/[uniqueName]/123
                String topicString = String.format("%s/hello/%s/%d", TOPIC_PREFIX,uniqueName,msgSeqNum);
                
                System.out.printf(">> Calling send() for #%d on %s%n",msgSeqNum,topicString);
                producer.send(message,JCSMPFactory.onlyInstance().createTopic(topicString));
                message.reset();     // reuse this message on the next loop, to avoid having to recreate it
                Thread.sleep(5000);  // take a pause
	        } catch (JCSMPException e) {
	            System.out.printf("### Exception caught during publish(): %s%n",e);
	            if (e instanceof JCSMPTransportException) {  // unrecoverable
	                isShutdown = true;
	            }
	        } catch (InterruptedException e) {
	            // IGNORE... probably getting shut down
	        }
        }
        System.out.println("Main thread quitting.");
        session.closeSession();  // quit right away
    }
}
