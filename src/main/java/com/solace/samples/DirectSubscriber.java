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

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;

public class DirectSubscriber {

    private static final String TOPIC_PREFIX = "solace/samples";  // used as the topic "root"

    private static boolean VERIFY_PAYLOAD_DATA = true;            // should we do some "processing" of incoming messages? (just for example)
    private static volatile int msgRecvCounter = 0;                   // num messages received
    private static volatile boolean hasDetectedDiscard = false;  // detected any discards yet?
    private static volatile boolean isShutdown = false;          // are we done?

    public static void main(String... args) throws JCSMPException, IOException, InterruptedException {
        // Check command line arguments
        if (args.length < 3) {
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [client-password]%n%n",
                    DirectSubscriber.class.getSimpleName());
            System.exit(-1);
        }

        System.out.println(DirectSubscriber.class.getSimpleName()+" initializing...");
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);          // host:port
        properties.setProperty(JCSMPProperties.VPN_NAME,  args[1]);     // message-vpn
        properties.setProperty(JCSMPProperties.USERNAME, args[2]);      // client-username
        if (args.length > 3) {
            properties.setProperty(JCSMPProperties.PASSWORD, args[3]);  // client-password
        }
        properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);  // re-subscribe Direct subs after reconnect
        JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
        channelProps.setReconnectRetries(20);      // recommended settings
        channelProps.setConnectRetriesPerHost(5);  // recommended settings
        // https://docs.solace.com/Solace-PubSub-Messaging-APIs/API-Developer-Guide/Configuring-Connection-T.htm
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES,channelProps);
        final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();
        
        /** Anonymous inner-class for MessageListener
         *  This demonstrates the async threaded message callback */
        final XMLMessageConsumer consumer = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage msg) {
                // do you want to do anything with this message?
                msgRecvCounter++;
                if (msg.getDiscardIndication()) {  // check if there have been any lost any messages
                    // If the consumer is being over-driven (i.e. publish rates too high), the broker might discard some messages for this consumer
                    // check this flag to know if that's happened
                    // to avoid discards:
                    //  a) reduce publish rate
                    //  b) use multiple-threads or shared subscriptions for parallel processing
                    //  c) increase size of consumer's D-1 egress buffers (check client-profile) (helps more with bursts)
                    hasDetectedDiscard = true;  // set my own flag
                }
                // this next block is just to have a non-trivial onReceive() callback... let's do a bit of work
                if (VERIFY_PAYLOAD_DATA) {
                    // as set in the publisher code, the payload should be filled with the same character as the last letter of the topic
                    if (msg instanceof BytesMessage && msg.getAttachmentContentLength() > 0) {  // non-empty BytesMessage
                        BytesMessage message = (BytesMessage)msg;  // cast the message (could also be TextMessage?)
                        byte[] payload = message.getData();        // get the payload
                        char payloadChar = (char)payload[0];       // grab the first byte/char
                        char lastTopicChar = message.getDestination().getName().charAt(message.getDestination().getName().length()-1);
                        if (payloadChar != lastTopicChar) {
                            System.out.println("*** Topic vs. Payload discrepancy *** : didn't match! oh well!");
                            VERIFY_PAYLOAD_DATA = false;  // don't do any further testing
                        }
                    } else {  // unexpected?
                        System.out.printf("vvv Received non-BytesMessage vvv%n%s%n",msg.dump());  // just print
                        VERIFY_PAYLOAD_DATA = false;  // don't do any further testing
                    }
                }
                if (msg.getDestination().getName().endsWith("control/quit")) {  // special sample message
                    System.out.println("QUIT message received, shutting down.");
                    isShutdown = true;
                }
            }

            @Override
            public void onException(JCSMPException e) {  // uh oh!
                System.err.println("### Exception thrown to the Consumer's onException()");
                e.printStackTrace();
                if (e instanceof JCSMPTransportException) {  // pretty bad, not recoverable
                	// means that all the reconnection attempts have failed
                	isShutdown = true;  // let's quit
                } else if (e instanceof JCSMPErrorResponseException) {  // might have some extra info
                    JCSMPErrorResponseException e1 = (JCSMPErrorResponseException)e;
                    System.out.println(JCSMPErrorResponseSubcodeEx.getSubcodeAsString(e1.getSubcodeEx())+": "+e1.getResponsePhrase());
                }
            }
        });

        session.addSubscription(JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX+"/>"));//direct/>"));
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX+"/control/>"));  // add multiple wildcard subscriptions
        consumer.start();
        System.out.println("Connected, and running...");
        try {
            while (System.in.available() == 0 && !isShutdown) {
                Thread.sleep(1000);  // wait 1 second
                System.out.printf("Received msgs/s: %,d%n",msgRecvCounter);  // simple way of calculating message rates
                msgRecvCounter = 0;
                if (hasDetectedDiscard) {
                    System.out.println("*** Egress discard detected *** : DirectSubscriber unable to keep up with full message rate");
                    hasDetectedDiscard = false;  // only show the error once per second
                }
            }
        } catch (InterruptedException e) {
            System.out.println("Main thread awoken while waiting");
        }
        System.out.println("Quitting in 1 second.");
        isShutdown = true;
        Thread.sleep(1000);
        // Close consumer
        consumer.close();
        session.closeSession();
    }
}
