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
import com.solacesystems.jcsmp.CapabilityType;
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

public class FastestPubDirect {

	// sdkperf java can do 175,594 msg/s @ 300B on a good system
    static final int MSG_RATE = 5000;  // Note: Standard edition PubSub+ broker limited to 10k max ingress
    static final int PAYLOAD_SIZE = 300;
    static volatile boolean shutdown = false;

    public static void main(String... args) throws JCSMPException, IOException, InterruptedException {
    	final int NANOS_BETWEEN_MSGS = 1_000_000_000 / MSG_RATE;

        // Check command line arguments
        if (args.length < 2 || args[1].split("@").length != 2) {
            System.out.println("Usage: FastestPubDirect <host:port> <client-username@message-vpn> [client-password]");
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

        System.out.println("FastestPubDirect initializing...");

        // Create a JCSMP Session
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);     // host:port
        properties.setProperty(JCSMPProperties.USERNAME, args[1].split("@")[0]); // client-username
        properties.setProperty(JCSMPProperties.VPN_NAME,  args[1].split("@")[1]); // message-vpn
        if (args.length > 2) { 
            properties.setProperty(JCSMPProperties.PASSWORD, args[2]); // client-password
        }
        JCSMPChannelProperties cp = new JCSMPChannelProperties();
        cp.setReconnectRetries(20);
        cp.setConnectRetriesPerHost(3);
        cp.setReconnectRetryWaitInMillis(2000);
        cp.setTcpNoDelay(false);
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
        for (CapabilityType ct : CapabilityType.values()) {
        	System.out.printf("%s = %s%n",ct.name(),session.getCapability(ct));
        }
        System.out.println(session.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES));

        Runnable pubThread = new Runnable() {
			@Override
			public void run() {
		        Topic topic = JCSMPFactory.onlyInstance().createTopic("aaron/test/pub");
		        BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
		        byte[] payload = new byte[PAYLOAD_SIZE];
		        long curNanoTime;
		        try {
		            while (!shutdown) {
		                curNanoTime = System.nanoTime();  // time at the start of this loop
		                Arrays.fill(payload,(byte)((Math.random()*26)+65));  // fill it with some "random" value
		                message.setData(payload);
		                prod.send(message,topic);
		                message.reset();  // reuse this message on the next loop, to avoid having to recreate it

		                while (System.nanoTime() < (curNanoTime + NANOS_BETWEEN_MSGS)) { }
		                    // BUSY WAIT!  (instead of Thread.sleep)
		                	// burns more CPU, but provides more accurate publish rate
		                	// comment out this "while" statement to go as fast as possible
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
		// here we go!
        Thread t = new Thread(pubThread,"Publisher Thread");
        t.setDaemon(true);
        t.start();
        System.out.println("Connected, and running. Press [ENTER] to quit.");
        System.in.read();  // wait for user to end
        System.out.println("Quitting in 1 second.");
        shutdown = true;
        Thread.sleep(1000);
    }
}
