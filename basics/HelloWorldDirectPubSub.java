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

package com.solace.samples.aaron.basics;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

/**
 * This simple introductory sample shows an application that both publishes and subscribes.
 * It includes some basic Solace "best practices", but for more substantial examples of Solace
 * applications, please refer to the other samples in this folder.
 */
public class HelloWorldDirectPubSub {
    
    private static volatile boolean shutdownFlag = false;      // are we done?
    private static final String TOPIC_PREFIX = "hello/world";  // used as the topic "root"
    private static String uniqueName = "default";              // change this later

    public static void main(String... args) throws JCSMPException, IOException, InterruptedException {
        if (args.length < 3) {  // Check command line arguments
            System.out.printf("Usage: HelloWorldDirectPubSub <host:port> <message-vpn>" +
                    " <client-username> [client-password]%n%n");
            System.exit(-1);
        }
        System.out.println("HelloWorldDirectPubSub initializing...");
        // Build the properties object(s) for initializing the Session
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);
        properties.setProperty(JCSMPProperties.VPN_NAME,  args[1]);
        properties.setProperty(JCSMPProperties.USERNAME, args[2]);
        if (args.length > 3) {
            properties.setProperty(JCSMPProperties.PASSWORD, args[3]);
        }
        properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);  // re-subscribe
        JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
        channelProps.setReconnectRetries(10);  // give more time for an HA failover
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES,channelProps);
        final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
        
        // User prompt, to use for specific topic
        System.out.print("Enter your name or a unique word: ");
        Scanner userInputScanner = new Scanner(System.in);
       	uniqueName = userInputScanner.next().trim().replaceAll("\\s+", "_");  // clean up whitespace
       	userInputScanner.close();
        
        // Producer callbacks config: simple anonymous inner-class for handling publishing events
        XMLMessageProducer producer = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
            // these first 2 are never called, they have been replaced by the "Ex" versions
            @Override public void responseReceived(String messageID) { }
            @Override public void handleError(String messageID, JCSMPException e, long timestamp) { }
            // next one only used in Guaranteed/Persistent publishing application
            @Override public void responseReceivedEx(Object key) { }
            // can be called for ACL violations, connection loss, and Persistent NACKs
            @Override
            public void handleErrorEx(Object key, JCSMPException e, long timestamp) {
                System.out.printf("### Producer handleErrorEx() callback: %s%n",e);
                if (e instanceof JCSMPTransportException) {  // unrecoverable
                    shutdownFlag = true;
                }
            }
        }, null);  // null is the ProducerEvent handler... don't need it in this simple application

        // Consumer callbacks next: anonymous inner-class for Listener async threaded callbacks
        final XMLMessageConsumer consumer = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage msg) {
                // could be 4 different message types: 3 SMF ones (Text, Map, Stream) and just plain binary
                System.out.printf("vvv RECEIVED A MESSAGE vvv%n%s%n",msg.dump());
                if (msg.getDestination().getName().equals("control/quit")) shutdownFlag = true;
            }

            @Override
            public void onException(JCSMPException e) {  // uh oh!
                System.out.printf("### MessageListener's onException(): %s%n",e);
                if (e instanceof JCSMPTransportException) {  // unrecoverable
                    shutdownFlag = true;
                }
            }
        });
        
        // Ready to start the application
        session.connect();  // connect to the broker
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX+"/>"));
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic("control/*"));  // to receive "quit" commands
        consumer.start();  // turn on the subs, and start receiving data

        final AtomicInteger msgSeqNum = new AtomicInteger();
        TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        while (!shutdownFlag) {  // time to loop!
            try {
            	// make an "interesting" payload
                String payloadText = String.format("message='%s'; time='%s'; sender='%s'; seq=%d",
                        "Hello World!", LocalDateTime.now(), uniqueName, msgSeqNum.incrementAndGet());
                message.setText(payloadText);//.getBytes(Charset.forName("UTF-8")));
                message.setSequenceNumber(msgSeqNum.get());
                Topic topic = JCSMPFactory.onlyInstance().createTopic(
                        String.format("%s/%s/%d",TOPIC_PREFIX,uniqueName,msgSeqNum.get()));
                System.out.printf(">> Calling send() for #%d on %s%n",msgSeqNum.get(),topic.getName());
                producer.send(message,topic);
                message.reset();  // reuse this message on the next loop, to avoid having to recreate it
                Thread.sleep(5000);  // take a pause
	        } catch (JCSMPException e) {
	            System.out.printf("### publisherRunnable publish() exception: %s%n",e);
	            if (e instanceof JCSMPTransportException) {  // unrecoverable
	                shutdownFlag = true;
	            }
	        } catch (InterruptedException e) {
	            // IGNORE... probably getting shut down
	        }
        }
        System.out.println("Main thread quitting.");
        session.closeSession();  // quit right away
    }
}
