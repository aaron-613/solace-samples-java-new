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
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProducerEventHandler;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.ProducerEventArgs;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class GuaranteedPublisher {
    
    
    
    /** Inner helper class object for managing ACKs and NACKs for published Guaranteed messages */
    public static class MessageAckInfo {
        
        public enum AckStatus {
            UNACK("UNACKed"),  // waiting
            ACK("ACKed"),      // success
            NACK("NACKed"),    // failure
            ;
            
            final String status;
            
            AckStatus(String status) {
                this.status = status;
            }
            
            @Override
            public String toString() {
                return String.format("%-7s",status);  // fixed-width, right-align the status
            }
        }
        
        private static AtomicLong myUniquePubSeqCount = new AtomicLong(1);  // or initialize from somewhere else
        
        private AckStatus ackStatus = AckStatus.UNACK;  // for now, when first published
        private final BytesMessage message;
        private final long id = myUniquePubSeqCount.getAndIncrement();
        
        public MessageAckInfo(BytesMessage message) {
            this.message = message;
        }
        
        public void ack() {
            ackStatus = AckStatus.ACK;
        }
        
        public void nack() {
            ackStatus = AckStatus.NACK;
        }
        
        public AckStatus getAckStatus() {
            return ackStatus;
        }
        
        @Override
        public String toString() {
            return String.format("MsgID: %d %s -- %s",id,ackStatus,message.toString());
        }
    }
    // END HELPER CLASS //////////////////////////////////////////////////////////

    
    /** Static inner class to keep code clean, used for handling ACKs/NACKs from broker **/
    private static class PublishCallbackHandler implements JCSMPStreamingPublishCorrelatingEventHandler {
        
        @Override
        public void responseReceived(String messageID) {
            // deprecated, superseded by responseReceivedEx()
        }
        
        @Override
        public void handleError(String messageID, JCSMPException e, long timestamp) {
            // deprecated, superseded by handleErrorEx()
        }
        
        @Override
        public void responseReceivedEx(Object key) {
            logger.info(String.format("ACK %d START: %s",((MessageAckInfo)key).id,key));
            assert key != null;  // this shouldn't happen, this should only get called for an ACK
            assert key instanceof MessageAckInfo;
            try {
                MessageAckInfo cKey = messagesAwaitingAcksRingBuffer.remove();  // will throw an exception if it's empty, should be impossible
                assert cKey == key;  // literally the same object!
                cKey.ack();  // we've received the ACK, so now we're done
            } finally {
                logger.info(String.format("  ACK %d END: %s",((MessageAckInfo)key).id,key));
            }
        }

        // can be called for ACL violations, connection loss, and Persistent NACKs
        @Override
        public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
            if (key == null) {
                System.out.printf("### Producer handleErrorEx() callback: %s%n",cause);
                if (cause instanceof JCSMPTransportException) {  // unrecoverable
                    isShutdown = true;
                } else if (cause instanceof JCSMPErrorResponseException) {  // might have some extra info
                    JCSMPErrorResponseException e = (JCSMPErrorResponseException)cause;
                    System.out.println(JCSMPErrorResponseSubcodeEx.getSubcodeAsString(e.getSubcodeEx())+": "+e.getResponsePhrase());
                }
                return;
            }  // else...
            System.out.println("### NACK received! "+key);
            assert key instanceof MessageAckInfo;
            try {
                MessageAckInfo cKey = messagesAwaitingAcksRingBuffer.remove();  // will throw an exception if it's empty, should be impossible
                assert cKey == key;
                cKey.nack();
                // now what?? Should we redlivery or something?  Maybe call some method or something as to what to do with this message?
            } catch (NoSuchElementException e) {
                throw new AssertionError("List was empty!",e);
            } finally {
                System.out.println("### NACK ending!");
            }
        }
    }
    //////////////////////////////////////////////////////
    
    private static final String SAMPLE_NAME = GuaranteedPublisher.class.getSimpleName();
    private static final String TOPIC_PREFIX = "solace/samples";  // used as the topic "root"
    private static final int PUBLISH_WINDOW_SIZE = 8;
    private static final Logger logger = LogManager.getLogger(GuaranteedPublisher.class);

    private static final ArrayBlockingQueue<MessageAckInfo> messagesAwaitingAcksRingBuffer = new ArrayBlockingQueue<>(10000);
    private static volatile boolean isShutdown = false;
    

    

    
    public static void main(String... args) throws JCSMPException, IOException, InterruptedException {
        if (args.length < 3) {  // Check command line arguments
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [client-password]%n%n",SAMPLE_NAME);
            System.out.println();
            System.exit(-1);
        }
        System.out.println(SAMPLE_NAME+" initializing...");

        // Create a JCSMP Session
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);     // host:port
        properties.setProperty(JCSMPProperties.VPN_NAME, args[1]); // message-vpn
        properties.setProperty(JCSMPProperties.USERNAME, args[2]); // client-username
        if (args.length > 3) { 
            properties.setProperty(JCSMPProperties.PASSWORD, args[3]); // client-password
        }
        properties.setProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR,true);
        properties.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE,PUBLISH_WINDOW_SIZE);
        JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
        channelProps.setReconnectRetries(20);      // recommended settings
        channelProps.setConnectRetriesPerHost(5);  // recommended settings
        // https://docs.solace.com/Solace-PubSub-Messaging-APIs/API-Developer-Guide/Configuring-Connection-T.htm
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES,channelProps);
        final JCSMPSession session =  JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();
        
        XMLMessageProducer producer = session.getMessageProducer(new PublishCallbackHandler(), new JCSMPProducerEventHandler() {
        @Override
        public void handleEvent(ProducerEventArgs event) {
            // as of Oct 2020, this event only occurs when republishing unACKed messages on an unknown flow
                System.out.println("*** Received a producer event: "+event);
            }
        });
        
        final int APPROX_MSG_RATE_PER_SEC = 1;
        final int PAYLOAD_SIZE = 100;

        Runnable pubThread = new Runnable() {
            @Override
            public void run() {
                Topic topic = JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX+"/pers/"+"asdf");
                byte[] payload = new byte[PAYLOAD_SIZE];
                try {
                    while (!isShutdown) {
//                        Arrays.fill(payload,(byte)(System.currentTimeMillis()%256));  // fill it with some "random" value
                        Arrays.fill(payload,(byte)65);  // fill it with some "random" value
                        // use a BytesMessage this sample, instead of TextMessage
                        BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
                        message.setData(payload);
                        message.setDeliveryMode(DeliveryMode.PERSISTENT);
                        message.setApplicationMessageId(UUID.randomUUID().toString());  // as an example
                        // let's define a user property!
                        SDTMap map = JCSMPFactory.onlyInstance().createMap();
                        map.putString("sample","JCSMP GuaranteedPublisher");
                        message.setProperties(map);
                        MessageAckInfo key = new MessageAckInfo(message);  // make a new structure for watching this message
                        message.setCorrelationKey(key);  // used for ACK/NACK correlation
                        try {
                            messagesAwaitingAcksRingBuffer.put(key);  // NEED NEED NEED to add it before the send() in case of race condition and the ACK comes back before it's added to the list
                            logger.info(String.format("           SEND %d START: %s",key.id,key));
                            producer.send(message,topic);  // message is *NOT* Guaranteed until ACK comes back to PublishCallbackHandler
                            logger.info(String.format("           SEND %d END:   %s",key.id,key));
                            Thread.sleep(000);
                            //Thread.sleep(1000/APPROX_MSG_RATE_PER_SEC);  // do Thread.sleep(0) for max speed
                            // Note: STANDARD Edition Solace PubSub+ broker is limited to 10k msg/s max ingress
                        } catch (InterruptedException e) {
                            isShutdown = true;
                        }

                    }
                } catch (JCSMPException e) {
                    e.printStackTrace();
                } finally {
                    System.out.println("Publisher Thread shutdown");
                }
            }
        };
        Thread t = new Thread(pubThread,"Publisher Thread");
        t.setDaemon(true);
        t.start();

        System.out.println("Connected, and running. Press [ENTER] to quit.");
        // block the main thread, waiting for a quit signal
        System.in.read();  // wait for user to end
        System.out.println("Quitting in 1 second.");
        isShutdown = true;
        Thread.sleep(1000);
    }
}
