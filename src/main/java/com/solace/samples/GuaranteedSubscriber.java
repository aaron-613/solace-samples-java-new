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

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.OperationNotSupportedException;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageListener;

public class GuaranteedSubscriber {
    
    private static final String SAMPLE_NAME = GuaranteedSubscriber.class.getSimpleName();
    private static final String QUEUE_NAME = "q_samples";
    
    private static volatile int msgRecvCounter = 0;                   // num messages received
    private static volatile boolean isShutdown = false;          // are we done?


    public static void main(String... args) throws JCSMPException, InterruptedException, IOException {
        if (args.length < 3) {  // Check command line arguments
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [client-password]%n%n",
                    SAMPLE_NAME);
            System.exit(-1);
        }
        System.out.println(SAMPLE_NAME+" initializing...");

        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);     // host:port
        properties.setProperty(JCSMPProperties.USERNAME, args[1]); // client-username
        properties.setProperty(JCSMPProperties.VPN_NAME,  args[2]); // message-vpn
        if (args.length > 3) {
            properties.setProperty(JCSMPProperties.PASSWORD, args[3]); // client-password
        }
        final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();

        // configure the queue API object locally
        final Queue queue = JCSMPFactory.onlyInstance().createQueue(QUEUE_NAME);

//        final EndpointProperties endpointProps = new EndpointProperties();
        // set queue permissions to "consume" and access-type to "exclusive"
//        endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
//        endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
        // Actually provision it, and do not fail if it already exists
        //session.provision(queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);

        System.out.printf("Attempting to bind to queue '%s' on the broker.%n", QUEUE_NAME);
//      EndpointProperties endpoint_props = new EndpointProperties();
//      endpoint_props.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);


        // Create a Flow be able to bind to and consume messages from the Queue.
        final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
        flow_prop.setEndpoint(queue);
        flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

        final FlowReceiver flowQueueReceiver;
        try {
            flowQueueReceiver = session.createFlow(new XMLMessageListener() {
                @Override
                public void onReceive(BytesXMLMessage msg) {
                    msgRecvCounter++;
    
                    // When the ack mode is set to SUPPORTED_MESSAGE_ACK_CLIENT,
                    // guaranteed delivery messages are acknowledged after
                    // processing
                    msg.ackMessage();
                }
    
                @Override
                public void onException(JCSMPException e) {
                    System.out.printf("Consumer received exception: %s%n", e);
                }
            }, flow_prop,null);
        } catch (OperationNotSupportedException e) {  // not allowed to do this
            throw e;
        } catch (JCSMPErrorResponseException e) {  // something else went wrong: queue not exist, queue shutdoown, etc.
            System.out.println("Could not establish a connection to the queue. Does it exist?");
            System.out.println("Create the queue using PubSub+ Manager GUI tool, or see the scripts inside the 'semp-rest-api' directory.");
            System.out.println();
            throw e;
        }

        // Start the consumer
        System.out.println("Connected. Awaiting message ...");
        flowQueueReceiver.start();

        try {
            while (System.in.available() == 0 && !isShutdown) {
                Thread.sleep(1000);  // wait 1 second
                System.out.printf("Received msgs/s: %,d%n",msgRecvCounter);  // simple way of calculating message rates
                msgRecvCounter = 0;
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
