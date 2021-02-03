/*
 * 
 * THIS SAMPLE IS NOT DONE YET!!!!
 * 
 */



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

package com.solace.samples.tbd;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.solacesystems.jcsmp.BytesXMLMessage;
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
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class GuaranteedReplier {

    /** Static inner class to keep code clean, used for handling ACKs/NACKs from broker **/
    private static class PublishCallbackHandler implements JCSMPStreamingPublishCorrelatingEventHandler {

        @Override
        public void responseReceivedEx(Object key) {
            assert key != null;  // this shouldn't happen, this should only get called for an ACK
            assert key instanceof BytesXMLMessage;
            logger.debug(String.format("ACK for Message %s",key));  // good enough, the broker has it now
        }

        @Override
        public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
            if (key != null) {  // NACK
                assert key instanceof BytesXMLMessage;
                logger.warn(String.format("NACK for Message %s",key));
                // probably want to do something here.  refer to sample xxxxxxx for error handling possibilities
                //  - send the message again
                //  - send it somewhere else (error handling queue?)
                //  - log and continue
                //  - pause and retry (backuoff)
                //  - attempt to rollback state, send a different message ot reset?
            } else {  // not a NACK, but some other error (ACL violation, connection loss, ...)
                System.out.printf("### Producer handleErrorEx() callback: %s%n",cause);
                if (cause instanceof JCSMPTransportException) {  // unrecoverable
                    isShutdown = true;
                } else if (cause instanceof JCSMPErrorResponseException) {  // might have some extra info
                    JCSMPErrorResponseException e = (JCSMPErrorResponseException)cause;
                    System.out.println(JCSMPErrorResponseSubcodeEx.getSubcodeAsString(e.getSubcodeEx())+": "+e.getResponsePhrase());
                    System.out.println(cause);
                }
            }
        }
    }
    //////////////////////////////////////////////////////
    
    private static final String SAMPLE_NAME = GuaranteedReplier.class.getSimpleName();
    private static final String TOPIC_PREFIX = "solace/samples";  // used as the topic "root"

    private static final Logger logger = LogManager.getLogger(GuaranteedReplier.class);  // log4j2, but could also use SLF4J, JCL, etc.

    private static volatile boolean isShutdown = false;

    /** The main method. */
    public static void main2b(String... args) throws JCSMPException, IOException {
        if (args.length < 3) {   // Check command line arguments
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [password]%n%n", SAMPLE_NAME);
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
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, channelProps);
        final JCSMPSession session;
        session = JCSMPFactory.onlyInstance().createSession(properties, null, new SessionEventHandler() {
            @Override
            public void handleEvent(SessionEventArgs event) {  // could be reconnecting, connection lost, etc.
                System.out.printf("### Received a Session event: %s%n",event);
            }
        });
        session.connect();

        final XMLMessageProducer producer = session.getMessageProducer(new PublishCallbackHandler());
        
        /** Anonymous inner-class for request handling **/
        final XMLMessageConsumer cons = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage requestMsg) {
                System.out.println(requestMsg.dump());
                if (requestMsg.getDestination().getName().contains("direct/request") && requestMsg.getReplyTo() != null) {
                    System.out.printf("Received request on '%s', generating response.%n",requestMsg.getDestination());
                    System.out.println(requestMsg.dump());
                    TextMessage replyMsg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);  // reply with a Text
                    if (requestMsg.getApplicationMessageId() != null) {
                        replyMsg.setApplicationMessageId(requestMsg.getApplicationMessageId());  // populate for traceability
                    }
                    final String text = "Hello! Here is a response to your message on topic '"+requestMsg.getDestination()+"'.";
                    replyMsg.setText(text);
                    try {
                        // only allowed to publish messages from API-owned (callback) thread when JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR == false
                        producer.sendReply(requestMsg, replyMsg);  // convenience method: copies in reply-to, correlationId, etc.
                    } catch (JCSMPException e) {
                        System.out.printf("### Caught while trying to producer.sendReply(): %s%n",e);
                        if (e instanceof JCSMPTransportException) {  // unrecoverable
                            isShutdown = true;
                        }
                    }
                } else {
                    System.out.println("Received message without reply-to field");
                }

            }

            public void onException(JCSMPException e) {
                System.out.printf("Consumer received exception: %s%n", e);
            }
        });

        // topic to listen to incoming (messaging) requests, using a special wildcard borrowed from MQTT:
        // https://docs.solace.com/Open-APIs-Protocols/MQTT/MQTT-Topics.htm#Using
        // will match "solace/samples/direct/request" as well as "solace/samples/direct/request/anything/else"
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX+"/direct/request/\u0003"));
        // for use with HTTP MicroGateway feature, will respond to REST GET request on same URI
        // try doing: curl -u default:default http://localhost:9000/solace/samples/direct/request/hello
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic("GET/"+TOPIC_PREFIX+"/direct/request/\u0003"));
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX+"/control/>"));
        cons.start();

        System.out.println(SAMPLE_NAME + " connected, and running. Press [ENTER] to quit.");
        try {
            while (System.in.available() == 0 && !isShutdown) {
                Thread.sleep(1000);  // wait 1 second
            }
        } catch (InterruptedException e) {
            // Thread.sleep() interrupted... probably getting shut down
        }
        System.out.println("Main thread quitting.");
        isShutdown = true;
        session.closeSession();  // will also close producer and consumer objects
    }
}
