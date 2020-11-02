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
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.OperationNotSupportedException;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.XMLMessageListener;

public class GuaranteedSubscriber {

    /** Very simple static inner class, used for receives messages from Queue Flows **/
    private static class QueueFlowListener implements XMLMessageListener {

        @Override
        public void onReceive(BytesXMLMessage msg) {
            msgRecvCounter++;
            if (msg.getRedelivered()) {
                // this is the broker telling the consumer that this message
                // has been sent and not ACKnowledge before
                // perhaps an error in processing? Should do extra checks to avoid duplicate processing
                hasDetectedRedelivery = true;
            }
            // Messages are removed from the broker queue when the ACK is received.
            // Therefore, do not ACK until all processing/storing of this message is complete.
            // NOTE that messages can be acknowledged from a different thread.
            msg.ackMessage();
        }

        @Override
        public void onException(JCSMPException e) {
            logger.warn("### Queue "+QUEUE_NAME+" Flow handler received exception", e);
            if (e instanceof JCSMPTransportException) {
                isShutdown = true;  // let's quit
            }
        }
    }
    
    
    private static final String SAMPLE_NAME = GuaranteedSubscriber.class.getSimpleName();
    private static final String QUEUE_NAME = "q_samples";
    
    private static volatile int msgRecvCounter = 0;                 // num messages received
    private static volatile boolean hasDetectedRedelivery = false;  // detected any messages being redelivered?
    private static volatile boolean isShutdown = false;             // are we done?

    private static final Logger logger = LogManager.getLogger(GuaranteedSubscriber.class);  // log4j2, but could also use SLF4J, JCL, etc.


    public static void main(String... args) throws JCSMPException, InterruptedException, IOException {
        if (args.length < 3) {  // Check command line arguments
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [client-password]%n%n",
                    SAMPLE_NAME);
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
        final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties,null,new SessionEventHandler() {
            @Override
            public void handleEvent(SessionEventArgs event) {  // could be reconnecting, connection lost, etc.
                logger.info("### Received a Session event: %s%n",event);
            }
        });
        session.connect();

        // configure the queue API object locally
        final Queue queue = JCSMPFactory.onlyInstance().createQueue(QUEUE_NAME);
        // Create a Flow be able to bind to and consume messages from the Queue.
        final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
        flow_prop.setEndpoint(queue);
        flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
        flow_prop.setActiveFlowIndication(true);  // Flow events will advise when 

        final FlowReceiver flowQueueReceiver;
        System.out.printf("Attempting to bind to queue '%s' on the broker.%n", QUEUE_NAME);
        try {
            flowQueueReceiver = session.createFlow(new QueueFlowListener(), flow_prop, null, new FlowEventHandler() {
                @Override
                public void handleEvent(Object source, FlowEventArgs event) {
                    // Flow events are usually: active, reconnecting (i.e. unbound), reconnected
                    logger.info("### Received a Flow event: %s%n",event);
                }
            });
        } catch (OperationNotSupportedException e) {  // not allowed to do this
            throw e;
        } catch (JCSMPErrorResponseException e) {  // something else went wrong: queue not exist, queue shutdown, etc.
            logger.error("Could not establish a connection to queue "+QUEUE_NAME+": "+e.toString());
            logger.error("Ensure queue exists using PubSub+ Manager WebGUI, or see the scripts inside the 'semp-rest-api' directory.");
            // could also try to retry, loop and retry until successfully able to connect to the queue
            return;
        }
        
        flowQueueReceiver.start();
        System.out.println(SAMPLE_NAME + " connected, and running. Press [ENTER] to quit.");
        try {
            while (System.in.available() == 0 && !isShutdown) {
                Thread.sleep(1000);  // wait 1 second
                System.out.printf("Received msgs/s: %,d%n",msgRecvCounter);  // simple way of calculating message rates
                msgRecvCounter = 0;
                if (hasDetectedRedelivery) {
                    System.out.println("*** Redelivery detected ***");
                    hasDetectedRedelivery = false;  // only show the error once per second
                }
            }
        } catch (InterruptedException e) {
            // Thread.sleep() interrupted... probably getting shut down
        }
        System.out.println("Main thread quitting.");
        isShutdown = true;
        flowQueueReceiver.stop();
        Thread.sleep(1000);
        session.closeSession();  // will also close consumer object
    }
}
