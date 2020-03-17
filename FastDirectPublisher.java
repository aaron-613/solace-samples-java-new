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
import java.util.Arrays;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProducerEventHandler;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.ProducerEventArgs;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class FastDirectPublisher {
    
    static final int MSG_RATE = 5000;  // Note: Standard edition PubSub+ broker is limited to 10k max ingress
    static final int PAYLOAD_SIZE = 300;
    static volatile boolean shutdown = false;

    public static void main(String... args) throws JCSMPException, IOException, InterruptedException {
        // Check command line arguments
        if (args.length < 2 || args[1].split("@").length != 2) {
            System.out.println("Usage: FastDirectPublisher <host:port> <client-username@message-vpn> [client-password]");
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

        System.out.println("FastDirectPublisher initializing...");

        // Create a JCSMP Session
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);     // host:port
        properties.setProperty(JCSMPProperties.USERNAME, args[1].split("@")[0]); // client-username
        properties.setProperty(JCSMPProperties.VPN_NAME,  args[1].split("@")[1]); // message-vpn
        if (args.length > 2) { 
            properties.setProperty(JCSMPProperties.PASSWORD, args[2]); // client-password
        }
        JCSMPChannelProperties cp = new JCSMPChannelProperties();
        cp.setTcpNoDelay(false);  // high throughput magic sauce... but will hurt latencies a bit
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES,cp);
        final JCSMPSession session =  JCSMPFactory.onlyInstance().createSession(properties);

        /** Anonymous inner-class for handling publishing events */
        XMLMessageProducer prod = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
            @Override
            public void responseReceived(String messageID) {
                // DIRECT publishers don't get publish callbacks
            }
            @Override
            public void handleError(String messageID, JCSMPException e, long timestamp) {
                // DIRECT publishers don't get publish callbacks
            }
        }, new JCSMPProducerEventHandler() {
            @Override
            public void handleEvent(ProducerEventArgs event) {
                System.out.println("Received a producer event: "+event);
                // should maybe do something with this?
            }
        });

        session.connect();

        Runnable pubThread = new Runnable() {
            @Override
            public void run() {
                final int APPROX_NANOS_BETWEEN_MSGS = (1_000_000_000 / MSG_RATE);  // won't be exactly the message rate, but close
                Topic topic = JCSMPFactory.onlyInstance().createTopic("aaron/test/topic");
                BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
                byte[] payload = new byte[PAYLOAD_SIZE];
                long curNanoTime = System.nanoTime();  // grab the current time before the start of the loop
                char characterOfTheMoment;
                try {
                    while (!shutdown) {
                        characterOfTheMoment = (char)((curNanoTime%26)+65);
                        Arrays.fill(payload,(byte)characterOfTheMoment);  // fill it with some "random" value
                        message.setData(payload);
                        // dynamic topics!! use StringBuilder because "+" concat operator is SLOW
                        topic = JCSMPFactory.onlyInstance().createTopic(new StringBuilder("aaron/test/").append(characterOfTheMoment).toString());
                        prod.send(message,topic);
                        message.reset();  // reuse this message on the next loop, to avoid having to recreate it

                        //while (System.nanoTime() < (curNanoTime + APPROX_NANOS_BETWEEN_MSGS)) { /* SPIN HARD */ }
                            // BUSY WAIT!  (instead of Thread.sleep)
                            // burns more CPU, but provides more accurate publish rate
                            // comment out the "while" statement to publish as fast as possible
                        curNanoTime = System.nanoTime();
                    }
                } catch (JCSMPException e) {
                    e.printStackTrace();
                } finally {
                    System.out.print("Shutdown! Stopping Publisher... ");
                    prod.close();
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
        shutdown = true;
        Thread.sleep(1000);
    }
}
