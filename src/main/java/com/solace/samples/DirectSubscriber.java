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
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPGlobalProperties;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;

public class DirectSubscriber {

    private static final boolean VERIFY_PAYLOAD_DATA = true;

    private static volatile int msgCounter = 0;
    private static volatile boolean discardFlag = false;
    private static volatile boolean shutdown = false;

    public static void main(String... args) throws JCSMPException, IOException, InterruptedException {
        // Check command line arguments
        if (args.length < 3) {
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [client-password]%n%n",
                    DirectSubscriber.class.getSimpleName());
            System.exit(-1);
        }

        System.out.println("DirectSubscriber initializing...");
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);     // host:port
        properties.setProperty(JCSMPProperties.VPN_NAME,  args[1]); // message-vpn
        properties.setProperty(JCSMPProperties.USERNAME, args[2]); // client-username
        if (args.length > 3) {
            properties.setProperty(JCSMPProperties.PASSWORD, args[3]); // client-password
        }
        JCSMPChannelProperties cp = new JCSMPChannelProperties();
        cp.setTcpNoDelay(false);  // high throughput magic sauce... but will hurt latencies a bit
        //cp.setCompressionLevel(9);
        cp.setReconnectRetries(0);
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, cp);
        {  // this next block is probably unnecessary until receiving > 100k msg/s, so delete if going slower than that
            JCSMPGlobalProperties gp = new JCSMPGlobalProperties();
            gp.setConsumerDefaultFlowCongestionLimit(20000);  // override default (5000)
            JCSMPFactory.onlyInstance().setGlobalProperties(gp);
        }
        final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
        
        /** Anonymous inner-class for MessageListener
         *  This demonstrates the async threaded message callback */
        final XMLMessageConsumer cons = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage msg) {
                // do you want to do anything with this message?
                msgCounter++;
                if (msg.getDiscardIndication()) {  // lost any messages?
                    // If the consumer is being over-driven (i.e. publish rates too high), the broker might discard some messages for this consumer
                    // check this flag to know if that's happened
                    // to avoid discards:
                    //  a) reduce publish rate
                    //  b) increase size of consumer's D-1 egress buffers (check client-profile)
                    //  c) use multiple-threads or shared subscriptions for parallel processing
                    discardFlag = true;  // set my own flag
                }
                System.out.println(msg.getSequenceNumber());
                // this next block is just to have a non-trivial onReceive() callback... let's do a bit of work
                if (VERIFY_PAYLOAD_DATA) {
                    // as set in the publisher code, the payload should be filled with the same character as the last letter of the topic
                    BytesMessage message = (BytesMessage)msg;
                    if (message.getAttachmentContentLength() > 0) {
                        byte[] payload = message.getData();
                        char payloadChar = (char)payload[0];
                        char lastTopicChar = message.getDestination().getName().charAt(message.getDestination().getName().length()-1);
                        if (payloadChar != lastTopicChar) {
                            System.out.println("*** Topic vs. Payload discrepancy *** : didn't match! oh well!");
                            //VERIFY_PAYLOAD_DATA = false;  // don't do any further testing
                        }
                    }
                }
            }

            @Override
            public void onException(JCSMPException e) {
                // uh oh!
                System.err.println("### Exception thrown to the Consumer's onException()");
                e.printStackTrace();
                if (e instanceof JCSMPTransportException) {  // pretty bad, not recoverable
                	// means that all the reconnection attempts have failed
                	shutdown = true;  // let's quit
                }
            }
        });

        session.connect();
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic("solace/direct/>"));
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic("solace/control/>"));
        cons.start();

        Runnable statsRunnable = new Runnable() {
            @Override
            public void run() {
                try {
                    while (!shutdown) {
                        Thread.sleep(1000);  // wait 1 second
                        System.out.printf("Msgs/s: %,d%n",msgCounter);
                        // simple way of calculating message rates
                        msgCounter = 0;
                        if (discardFlag) {
                            System.out.println("*** Egress discard detected *** : Direct Subscriber unable to keep up with full message rate");
                            discardFlag = false;  // only show the error once per second
                        }
                    }
                } catch (InterruptedException e) {
                    System.out.println("I was awoken while waiting");
                }
            }
        };
        Thread t = new Thread(statsRunnable,"Stats Thread");
        t.setDaemon(true);
        t.start();
        
        System.out.println("Connected, and running. Press [ENTER] to quit.");
        System.in.read();  // wait for user to end
        System.out.println("Quitting in 1 second.");
        shutdown = true;
        Thread.sleep(1000);
        // Close consumer
        cons.close();
        session.closeSession();
    }
}
