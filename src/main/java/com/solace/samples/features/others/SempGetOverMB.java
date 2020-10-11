/**
 * SempGetOverMB.java
 * 
 * This sample demonstrates simple SEMP requests over the message bus. 
 * It performs a request and prints the response.
 * Sample requirements:
 *  - A Solace appliance running SolOS-TR.
 *  - When running with SolOS-TR 5.3.1 and above , The client's message vpn must have semp-over-msgbus enabled for SHOW commands.
 *  - The client's message vpn must have management-message-vpn enabled to send SEMP requests outside of its message vpn.
 *  
 * Copyright 2009-2020 Solace Corporation. All rights reserved.
 */

package com.solace.samples.features.others;

import java.util.Map;

import com.solace.samples.features.common.ArgParser;
import com.solace.samples.features.common.SampleApp;
import com.solace.samples.features.common.SampleUtils;
import com.solace.samples.features.common.SessionConfiguration;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.CapabilityType;
import com.solacesystems.jcsmp.Consumer;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.Requestor;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class SempGetOverMB extends SampleApp {
	Consumer cons = null;
    XMLMessageProducer prod = null;
	SessionConfiguration conf = null;

	void createSession(String[] args) {
		ArgParser parser = new ArgParser();
		
		// Parse command-line arguments.
		if (parser.parse(args) == 0)
			conf = parser.getConfig();
		else
			printUsage(parser.isSecure());

		session = SampleUtils.newSession(conf, new PrintingSessionEventHandler(),null);
	}
	
	void printUsage(boolean secure) {
        String strusage = ArgParser.getCommonUsage(secure);
        strusage += "This sample:\n";
        strusage += "\t[-sv SEMP_VERSION] \tSEMP version in the SEMP request. Default: " + SEMP_VERSION_TR + "\n";
        System.out.println(strusage);
		finish(1);
	}
	
	public SempGetOverMB() {
	}

	public static void main(String[] args) {
		SempGetOverMB bsub = new SempGetOverMB();
		bsub.run(args);
	}
	
	void run(String[] args) {
		createSession(args);

		try {
			// Create a producer and a consumer, and connect to appliance.
			System.out.println("About to connect to appliance.");
	        session.connect();
			prod = session.getMessageProducer(new PrintingPubCallback());
			cons = session.getMessageConsumer(new PrintingMessageHandler());
			printRouterInfo();
			cons.start();
			System.out.println("Connected!");
			
			// Extract the router name. 
			String routerName = (String) session.getCapability(CapabilityType.PEER_ROUTER_NAME);

			final String SEMP_TOPIC_STRING = String.format("#SEMP/%s/SHOW", routerName);
			final Topic SEMP_TOPIC = JCSMPFactory.onlyInstance().createTopic(SEMP_TOPIC_STRING);
			
			Map<String,String> extraArguments = conf.getArgBag();
			String sempVersion = extraArguments.containsKey("-sv") ? sempVersion = extraArguments.get("-sv") : SEMP_VERSION_TR;
			final String SEMP_SHOW_CLIENT_NAME = "<rpc semp-version=\"" + sempVersion + "\"><show><client><name>*</name></client></show></rpc>";

			System.out.printf("Router name is '%s', SEMP topic address is '%s'\n", routerName, SEMP_TOPIC_STRING);			
			
			// Set up the requestor and request message.
			Requestor requestor = session.createRequestor();
			BytesXMLMessage requestMsg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
			requestMsg.writeAttachment(SEMP_SHOW_CLIENT_NAME.getBytes());

			System.out.println("REQUEST: " + SEMP_SHOW_CLIENT_NAME);
			System.out.println("REQUEST ADDRESS: " + SEMP_TOPIC);
			BytesXMLMessage replyMsg = requestor.request(requestMsg, 5000, SEMP_TOPIC);

			String replyStr = new String();
			if (replyMsg.getAttachmentContentLength() > 0) {
				byte[] bytes = new byte[replyMsg.getAttachmentContentLength()];
				replyMsg.readAttachmentBytes(bytes);
				replyStr = new String(bytes, "US-ASCII");
			}
			System.out.println("REPLY: " + replyStr);
			
			if (replyStr.contains("<permission-error>")) {
	            System.out.println("Permission Error: Make sure SEMP over message bus SHOW commands are enabled for this VPN");
			}
			finish(0);
		} catch (JCSMPTransportException ex) {
			System.err.println("Encountered a JCSMPTransportException, closing consumer channel... " + ex.getMessage());
			if (cons != null) {
				cons.close();
				// At this point the consumer handle is unusable; a new one should be created 
				// by calling cons = session.getMessageConsumer(...) if the application 
				// logic requires the consumer channel to remain open.
			}
			finish(1);
		} catch (JCSMPException ex) {
			System.err.println("Encountered a JCSMPException, closing consumer channel... " + ex.getMessage());
			// Possible causes: 
			// - Authentication error: invalid username/password 
			// - Provisioning error: unable to add subscriptions from CSMP
			// - Invalid or unsupported properties specified
			if (cons != null) {
				cons.close();
				// At this point the consumer handle is unusable, a new one should be created 
				// by calling cons = session.getMessageConsumer(...) if the application 
				// logic requires the consumer channel to remain open.
			}
			finish(1);
		} catch (Exception ex) {
			System.err.println("Encountered an Exception... " + ex.getMessage());
			finish(1);
		}
	}

}
