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

import java.util.Arrays;
import java.util.LinkedList;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.Message;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class FastPublisherGuaranteed {

    private static volatile LinkedList<Message> sentMessages = new LinkedList<>();

    public static void main(String... args) throws JCSMPException {

        // Check command line arguments
        if (args.length < 2 || args[1].split("@").length != 2) {
            System.out.println("Usage: FastPublisherGuaranteed <host:port> <client-username@message-vpn> [client-password]");
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

        System.out.println("FastPublisherGuaranteed initializing...");

        // Create a JCSMP Session
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);     // host:port
        properties.setProperty(JCSMPProperties.USERNAME, args[1].split("@")[0]); // client-username
        properties.setProperty(JCSMPProperties.VPN_NAME,  args[1].split("@")[1]); // message-vpn
        if (args.length > 2) { 
            properties.setProperty(JCSMPProperties.PASSWORD, args[2]); // client-password
        }
        final JCSMPSession session =  JCSMPFactory.onlyInstance().createSession(properties);

        session.connect();

        /** Anonymous inner-class for handling publishing events */
        XMLMessageProducer prod = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
            @Override
            public void responseReceived(String messageID) {
            	// this is a regular ACK... good stuff!
                //System.out.println("Producer received response for msg: " + messageID);
            	Message msg = sentMessages.remove();
            	System.out.println("ACK:  "+msg);
            	System.out.println("callback msgID: "+messageID+", actual msgID: "+msg.getMessageId());
            	System.out.println("------------------");
            }
            @Override
            public void handleError(String messageID, JCSMPException e, long timestamp) {
            	// not good... this is a NACK... happens if a queue fills up or some other publishing error
                System.err.printf("Producer received error for msg: %s @ %s - %s%n",messageID,timestamp,e);
                Message msg = sentMessages.remove();
                System.err.println("This one: "+msg);
                System.err.println("Exiting!");
                System.exit(-1);  // obviously not ideal... should instead try to resend the message, delay, try again in a few seconds
            }
        });

        final double MSG_RATE = 0.5;
        final int PAYLOAD_SIZE = 300;

    	Topic topic = JCSMPFactory.onlyInstance().createTopic("a/b/c");
        byte[] payload = new byte[PAYLOAD_SIZE];
        long curNanoTime;
        
        try {
	        while (true) {
	        	curNanoTime = System.nanoTime();  // time at the start of this loop
//	        	Arrays.fill(payload,(byte)(System.currentTimeMillis()%256));  // fill it with some "random" value
	        	Arrays.fill(payload,(byte)32);  // fill it with some "random" value
	            BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
	        	message.setData(payload);
	        	message.setDeliveryMode(DeliveryMode.PERSISTENT);
	        	prod.send(message,topic);
	        	System.out.println("SENT: "+message);
	        	sentMessages.add(message);
	        	while (System.nanoTime() < (curNanoTime + (1_000_000_000 / MSG_RATE))) {
	        		// busy wait
	        	}
	        }
        } catch (JCSMPException e) {
        	throw e;
        } finally {
        	prod.close();
        	session.closeSession();
        }
    }
}
