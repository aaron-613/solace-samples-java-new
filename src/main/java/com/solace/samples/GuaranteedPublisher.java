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
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProducerEventHandler;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.ProducerEventArgs;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class GuaranteedPublisher {
    
    public static class MessageInfo {
        
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
        
        private BytesMessage message;
        private AckStatus ackStatus = AckStatus.UNACK;  // for now, when first published
        private long myUniqueId = myUniquePubSeqCount.getAndIncrement();
        
        public MessageInfo(BytesMessage message) {
            this.message = message;
        }
        
        public long getId() {
            return myUniqueId;
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
        
        public BytesMessage getMessage() {
            return message;
        }
        
        @Override
        public String toString() {
            return String.format("MsgID: %d %s -- %s",myUniqueId,ackStatus,message.toString());
        }
        
    }
    
    private static final String TOPIC_PREFIX = "solace/samples";  // used as the topic "root"
    private static final int PUBLISH_WINDOW_SIZE = 1;
    private static final Logger logger = LogManager.getLogger(GuaranteedPublisher.class);

    private static volatile LinkedList<MessageInfo> messagesAwaitingAckList = new LinkedList<>();
    private static final ArrayBlockingQueue<MessageInfo> messagesAwaitingRingBuffer = new ArrayBlockingQueue<>(PUBLISH_WINDOW_SIZE+1);
    
    private static final ArrayBlockingQueue<BytesMessage> messagesPoolRingBuffer = new ArrayBlockingQueue<>(PUBLISH_WINDOW_SIZE+1);
//    static {
//        for (int i=0;i<PUBLISH_WINDOW_SIZE;i++) {
//            messagesPoolRingBuffer.add(JCSMPFactory.onlyInstance().createMessage(BytesMessage.class));
//        }
//    }
    private static volatile boolean isShutdown = false;

//    static volatile long lastTs = System.nanoTime() / 1_000_000;
//    static synchronized String getTs() {
//        long ts = System.nanoTime() / 1_000_000;  // milliseconds
//        try {
//            if (ts - lastTs > 100) {
//                return String.format(" -- gap --%n%d",ts);
//            }
//            return Long.toString(ts);
//        } finally {
//            lastTs = ts;
//        }
//    }
    
    
    static class PublishCallback implements JCSMPStreamingPublishCorrelatingEventHandler {
        
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
            if (key == null) {
                System.err.println("NULL KEY IN responseReceivedEx()");
                return;
            }
//            System.out.printf("%s   ACK %d START: %s%n",getTs(),((MessageInfo)key).myUniqueId,key);
            MessageInfo cKey;
            try {
                cKey = messagesAwaitingAckList.remove();
                if (!cKey.equals(key)) throw new AssertionError("Unexpected key, wrong order!");
                cKey.ack();
                cKey.message.reset();
                messagesPoolRingBuffer.add(cKey.message);  // done with this message, now add it back to the pool
            } catch (NoSuchElementException e) {
                throw new AssertionError("List was empty!",e);
            } finally {
//                System.out.printf("%s   ACK %d END:   %s%n",getTs(),((MessageInfo)key).myUniqueId,key);
            }
        }

        // this method could be run for other things besides a NACK, so keep that in mind
        @Override
        public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
            if (key == null) {
                System.err.println("NULL KEY IN handleErrorEx()");
                cause.printStackTrace();
                return;
            }
            System.out.println("NACK received: "+key);
            MessageInfo cKey;
            try {
                cKey = messagesAwaitingAckList.remove();
                if (!key.equals(cKey)) throw new AssertionError("Unexpected key, wrong order!");
                cKey.nack();
                // now what?? Should we redlivery or something?  Maybe call some method or something as to what to do with this message?
                messagesPoolRingBuffer.add(JCSMPFactory.onlyInstance().createMessage(BytesMessage.class));
            } catch (NoSuchElementException e) {
                throw new AssertionError("List was empty!",e);
            } catch (NullPointerException e) {
                throw new AssertionError("This should be impossible, key ==  null!",e);
            } finally {
                System.out.println("NACK ending!");
            }
        }
    }

    
    public static void main(String... args) throws JCSMPException, IOException, InterruptedException {

        // Check command line arguments
        if (args.length < 3) {
            System.out.println("Usage: GuaranteedPublisher <host:port> <message-vpn> <client-username> [client-password]");
            System.out.println();
            System.exit(-1);
        }

        System.out.println("GuaranteedPublisher initializing...");

        // Create a JCSMP Session
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);     // host:port
        properties.setProperty(JCSMPProperties.VPN_NAME, args[1]); // message-vpn
        properties.setProperty(JCSMPProperties.USERNAME, args[2]); // client-username
        if (args.length > 3) { 
            properties.setProperty(JCSMPProperties.PASSWORD, args[3]); // client-password
        }
        properties.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE,PUBLISH_WINDOW_SIZE);
        JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
        channelProps.setReconnectRetries(20);      // recommended settings
        channelProps.setConnectRetriesPerHost(5);  // recommended settings
        // https://docs.solace.com/Solace-PubSub-Messaging-APIs/API-Developer-Guide/Configuring-Connection-T.htm
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES,channelProps);
        final JCSMPSession session =  JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();
        
        XMLMessageProducer producer = session.getMessageProducer(new PublishCallback(), new JCSMPProducerEventHandler() {
        @Override
        public void handleEvent(ProducerEventArgs event) {
            // as of Oct 2020, this event only occurs when republishing unACKed messages on an unknown flow
                System.out.println("*** Received a producer event: "+event);
            }
        });
        
        final double MSG_RATE = 1;
        final int PAYLOAD_SIZE = 100;

        Runnable pubThread = new Runnable() {
            @Override
            public void run() {
                Topic topic = JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX+"/pers/"+"a/b/c");
                byte[] payload = new byte[PAYLOAD_SIZE];
                long curNanoTime;
                try {
                    while (!isShutdown) {
                        curNanoTime = System.nanoTime();  // time at the start of this loop
//                        Arrays.fill(payload,(byte)(System.currentTimeMillis()%256));  // fill it with some "random" value
                        Arrays.fill(payload,(byte)65);  // fill it with some "random" value
                        // use a BytesMessage this sample, instead of TextMessage
                        BytesMessage message = messagesPoolRingBuffer.poll();
                        if (message == null) {  // somehow the ring buffer is empty????
                            System.out.println("EMPTY RING BUFFER WHAT??????? Should be impossible due to AD publish window size");
                            //throw new AssertionError("EMPTY RING BUFFER");
                            message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);  // make a new message object
                        }
                        message.setData(payload);
                        message.setDeliveryMode(DeliveryMode.PERSISTENT);
                        message.setApplicationMessageId(UUID.randomUUID().toString());  // as an example
                        MessageInfo key = new MessageInfo(message);  // make a new structure for watching this message
                        message.setCorrelationKey(key);
                        messagesAwaitingAckList.add(key);  // NEED NEED NEED to add it before the send() in case of race condition and the ACK comes back before it's added to the list
                        
//                        System.out.printf("%s  SEND %d START: %s%n",getTs(),key.myUniqueId,key);
                        producer.send(message,topic);
//                        System.out.printf("%s  SEND %d END:   %s%n",getTs(),key.myUniqueId,key);
                        
                        //while (System.nanoTime() < (curNanoTime + (1_000_000_000 / MSG_RATE))) { }
                            // busy wait
                    }
                } catch (JCSMPException e) {
                    e.printStackTrace();
                } finally {
                    System.out.print("Shutdown! Stopping Publisher... ");
                    producer.close();
                    session.closeSession();
                    System.out.println("Done.");
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
