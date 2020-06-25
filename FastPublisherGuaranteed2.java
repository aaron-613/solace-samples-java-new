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

package com.solace.samples.aaron;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class FastPublisherGuaranteed2 {
	
	public static class CorrelationKey {
		
		public enum AckStatus {
			UNACK("UNACKed"),
			ACK("ACKed"),
			NACK("NACKed"),
			;
			
			final String status;
			
			AckStatus(String status) {
				this.status = status;
			}
			
			@Override
			public String toString() {
				return String.format("%-7s",status);
			}
			
		}
		
		private static AtomicLong uniquePubSeqCount = new AtomicLong(1);
		private final BytesXMLMessage message;
		private AckStatus acknowledged = AckStatus.UNACK;
		private final long id = uniquePubSeqCount.getAndIncrement();
		
		public CorrelationKey(BytesXMLMessage message) {
			this.message = message;
		}
		
		public long getId() {
			return id;
		}
		
		public void ack() {
			acknowledged = AckStatus.ACK;
		}
		
		public void nack() {
			acknowledged = AckStatus.NACK;
		}
		
		public AckStatus getAcknowledged() {
			return acknowledged;
		}
		
		public BytesXMLMessage getMessage() {
			return message;
		}
		
		@Override
		public String toString() {
			return String.format("MsgID: %d %s -- %s",id,acknowledged,message.toString());
		}
		
	}

    private static volatile LinkedList<CorrelationKey> messagesAwaitingAckList = new LinkedList<>();
    private static volatile boolean shutdown = false;

    static volatile long lastTs = System.nanoTime() / 1_000_000;
    static synchronized String getTs() {
    	long ts = System.nanoTime() / 1_000_000;  // milliseconds
    	try {
	    	if (ts - lastTs > 100) {
	        	return String.format(" -- gap --%n%d",ts);
	    	}
	    	return Long.toString(ts);
    	} finally {
    		lastTs = ts;
    	}
    }
    
    public static void main(String... args) throws JCSMPException, IOException, InterruptedException {

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
        properties.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE,5);
        final JCSMPSession session =  JCSMPFactory.onlyInstance().createSession(properties);

        session.connect();

        /** Anonymous inner-class for handling publishing events */
        final XMLMessageProducer prod = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
        	
            @Override
            public void responseReceived(String messageID) {
            	throw new AssertionError("This should never be called!");
            }
            
            @Override
            public void handleError(String messageID, JCSMPException e, long timestamp) {
            	throw new AssertionError("This should never be called!");
            }
            
			@Override
			public void responseReceivedEx(Object key) {
	        	System.out.printf("%s   ACK %d START: %s%n",getTs(),((CorrelationKey)key).id,key);
				CorrelationKey cKey;
				try {
                    cKey = messagesAwaitingAckList.remove();
                    if (!cKey.equals(key)) throw new AssertionError("Unexpected key, wrong order!");
                    cKey.ack();
				} catch (NoSuchElementException e) {
					throw new AssertionError("List was empty!",e);
				} finally {
		        	System.out.printf("%s   ACK %d END:   %s%n",getTs(),((CorrelationKey)key).id,key);
				}
			}

			@Override
			public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
	        	System.out.println("NACK received: "+key);
				CorrelationKey cKey;
				try {
                    cKey = messagesAwaitingAckList.remove();
                    if (!key.equals(cKey)) throw new AssertionError("Unexpected key, wrong order!");
                    cKey.nack();
                    // now what?? Should we redlivery or somtehing?
				} catch (NoSuchElementException e) {
					throw new AssertionError("List was empty!",e);
				} catch (NullPointerException e) {
					throw new AssertionError("This should be impossible, key ==  null!",e);
				} finally {
					System.out.println("NACK ending!");
				}
			}
			
        });
        
        final double MSG_RATE = 1;
        final int PAYLOAD_SIZE = 100;

        Runnable pubThread = new Runnable() {
			@Override
			public void run() {
		    	Topic topic = JCSMPFactory.onlyInstance().createTopic("a/b/c");
		        byte[] payload = new byte[PAYLOAD_SIZE];
		        long curNanoTime;
		        try {
			        while (!shutdown) {
			        	curNanoTime = System.nanoTime();  // time at the start of this loop
//			        	Arrays.fill(payload,(byte)(System.currentTimeMillis()%256));  // fill it with some "random" value
			        	Arrays.fill(payload,(byte)65);  // fill it with some "random" value
			            BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
			        	message.setData(payload);
			        	message.setDeliveryMode(DeliveryMode.PERSISTENT);
			        	CorrelationKey key = new CorrelationKey(message);
			        	message.setCorrelationKey(key);
			        	message.setApplicationMessageId(Long.toString(key.getId()));  // maybe a GUID would be better?  For track-and-trace?
			        	messagesAwaitingAckList.add(key);  // NEED NEED NEED to add it before the send() in case of race condition and the ACK comes back before it's added to the list
			        	
			        	System.out.printf("%s  SEND %d START: %s%n",getTs(),key.id,key);
			        	prod.send(message,topic);
			        	System.out.printf("%s  SEND %d END:   %s%n",getTs(),key.id,key);
			        	
			        	//while (System.nanoTime() < (curNanoTime + (1_000_000_000 / MSG_RATE))) { }
			        		// busy wait
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
