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

package com.solace.samples;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

/** Guaranteed Messaging sample to demonstrate initiating a request-reply flow.
 *  Not entirely 
 */
public class GuaranteedRequestorAsync {
    
    
    private static final String SAMPLE_NAME = GuaranteedRequestorAsync.class.getSimpleName();
    private static final String TOPIC_PREFIX = "solace/samples";  // used as the topic "root"
    private static final int REQUEST_TIMEOUT_MS = 5000;  // time to wait for a reply before timing out

    private static final Logger logger = LogManager.getLogger(GuaranteedRequestorAsync.class);  // log4j2, but could also use SLF4J, JCL, etc.

    private static volatile int msgSentCounter = 0;                   // num messages sent
    private static volatile boolean isShutdown = false;

    public static void main2b(String... args) throws JCSMPException, IOException {
        if (args.length < 3) {  // Check command line arguments
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [password]%n%n", SAMPLE_NAME);
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
        final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties,null,new SessionEventHandler() {
            @Override
            public void handleEvent(SessionEventArgs event) {  // could be reconnecting, connection lost, etc.
                System.out.printf("### Received a Session event: %s%n",event);
            }
        });
        session.connect();

        final XMLMessageProducer producer = session.getMessageProducer(new PublishCallbackHandler());

//        final XMLMessageConsumer consumer = session.getMessageConsumer((XMLMessageListener)null);  // less common use of synchronous consumer mode
//        consumer.start();  // needed to accept the responses

        Queue replyQueue = session.createTemporaryQueue();
        
        ConsumerFlowProperties flowProps = new ConsumerFlowProperties();
        flowProps.setEndpoint(replyQueue);
        FlowReceiver flow = session.createFlow(null, flowProps);
        flow.start();

        System.out.println(SAMPLE_NAME + " connected, and running. Press [ENTER] to quit.");
        TextMessage requestMsg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        try {
            while (System.in.available() == 0 && !isShutdown) {
                try {
                    requestMsg.setText(String.format("Hello, this is reqeust #%d",msgSentCounter));
                    requestMsg.setDeliveryMode(DeliveryMode.PERSISTENT);
                    requestMsg.setCorrelationId(String.format("REQ#%d",msgSentCounter));
                    Topic topic = JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX+"/pers/request");  // could also send directly to a queue
                    System.out.printf("About to send request message #%d to topic '%s'...%n",msgSentCounter,topic.getName());
                    producer.send(requestMsg, topic);  // send and receive in one call
                    msgSentCounter++;  // add one
                    BytesXMLMessage replyMsg = flow.receive(REQUEST_TIMEOUT_MS);
                    if (replyMsg != null) {  // didn't get a response!
                        System.out.printf("Response Message Dump:%n%s%n",replyMsg.dump());
                    } else {
                        System.out.println("Failed to receive a reply in " + REQUEST_TIMEOUT_MS + " msecs");
                    }
                    requestMsg.reset();  // reuse this message, to avoid having to recreate it: better performance
                    Thread.sleep(1000);  // approx. one request per second
                } catch (JCSMPException e) {  // threw from send(), only thing that is throwing here, but keep trying (unless shutdown?)
                    System.out.printf("### Caught while trying to producer.send(): %s%n",e);
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



    /** Static inner class to keep code clean, used for handling ACKs/NACKs from broker. **/
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


    ////////////////////////////////////////////////////////////////////////////

    /** Very simple static inner class, used for receives messages from Queue Flows. **/
    private static class QueueFlowListener implements XMLMessageListener {

        private static final String QUEUE_NAME = "asdf";

        @Override
        public void onReceive(BytesXMLMessage msg) {

            msg.ackMessage();  // ACKs are asynchronous
        }

        @Override
        public void onException(JCSMPException e) {
            logger.warn("### Queue " + QUEUE_NAME + " Flow handler received exception", e);
            if (e instanceof JCSMPTransportException) {
                isShutdown = true;  // let's quit
            }
        }
    }


}
