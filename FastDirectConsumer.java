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

package com.solace.samples.perf;

import java.io.IOException;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPGlobalProperties;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;

public class FastDirectConsumer {

    private static volatile int msgCounter = 0;
    private static volatile boolean discardFlag = false;
    private static volatile boolean doSomeSillyVerifyingOfData = true;
    private static volatile boolean shutdown = false;

    public static void main(String... args) throws JCSMPException, IOException, InterruptedException {

        // Check command line arguments
        if (args.length < 2 || args[1].split("@").length != 2) {
            System.out.println("Usage: FastDirectConsumer <host:port> <client-username@message-vpn> [client-password]");
            System.out.println();
            System.exit(-1);
        }
        if (args[1].split("@")[0].isEmpty()) {
            System.out.println("No client-username entered");
            System.out.println();
            System.exit(-1);
        }
        if (args[1].split("@")[1].isEmpty()) {
            System.out.println("No message-vpn entered");
            System.out.println();
            System.exit(-1);
        }

        System.out.println("FastDirectConsumer initializing...");
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);     // host:port
        properties.setProperty(JCSMPProperties.USERNAME, args[1].split("@")[0]); // client-username
        properties.setProperty(JCSMPProperties.VPN_NAME,  args[1].split("@")[1]); // message-vpn
        if (args.length > 2) {
            properties.setProperty(JCSMPProperties.PASSWORD, args[2]); // client-password
        }
        //properties.setProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR,true);  // use only 1 thread instead of 2, but hurts throughput 
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
                // this next block is just to have a non-trivial onReceive() callback... let's do a bit of work
                if (doSomeSillyVerifyingOfData) {
                    // as set in the publisher code, the payload should be filled with the same character as the last letter of the topic
                    BytesMessage message = (BytesMessage)msg;
                    if (message.getAttachmentContentLength() > 0) {
                        byte[] payload = message.getData();
                        char payloadChar = (char)payload[0];
                        char lastTopicChar = message.getDestination().getName().charAt(message.getDestination().getName().length()-1);
                        if (payloadChar != lastTopicChar) {
                            System.out.println("*** Topic vs. Payload discrepancy *** : didn't match! oh well!");
                            doSomeSillyVerifyingOfData = false;  // don't do any further testing
                        }
                    }
                }
            }

            @Override
            public void onException(JCSMPException e) {
                // uh oh!
                System.err.println("We've had an exception thrown to the Consumer's onException()");
                e.printStackTrace();
            }
        });

        session.connect();
        final Topic topic = JCSMPFactory.onlyInstance().createTopic("aaron/>");
        session.addSubscription(topic);
        cons.start();

        Runnable statsThread = new Runnable() {
            @Override
            public void run() {
                try {
                    while (!shutdown) {
                        Thread.sleep(1000);  // wait 1 second
                        System.out.printf("Msgs/s: %,d%n",msgCounter);
                        // kinda hacky way of doing message rates, but doesn't matter too much
                        msgCounter = 0;
                        if (discardFlag) {
                            System.out.println("*** Egress discard detected *** : consumer unable to keep up with full message rate");
                            discardFlag = false;  // only show the error once per second
                        }
                    }
                } catch (InterruptedException e) {
                    System.out.println("I was awoken while waiting");
                }
            }
        };
        Thread t = new Thread(statsThread,"Stats Thread");
        t.setDaemon(true);
        t.start();
        
        System.out.println("Connected, and running. Press [ENTER] to quit.");
        System.in.read();  // wait for user to end
        System.out.println("Quitting in 1 second.");
        shutdown = true;
        session.removeSubscription(topic);
        Thread.sleep(1000);
        // Close consumer
        cons.close();
        session.closeSession();
    }
}
