/**
 * SempPagingRequests.java
 * 
 * This sample demonstrates SEMP requests with paging. The SEMP request shows configured 
 * queues on a Solace appliance, in sets of five per response.
 * 
 * The sample shows how to extract the more-cookie (used for paging) from 
 * a response to get more data by performing more requests.
 * Sample requirements:
 *  - A Solace appliance running SolOS-TR.
 *  - When running with SolOS-TR 5.3.1 and above , The client's message vpn must have semp-over-msgbus enabled.
 *  - The client's message vpn must have management-message-vpn enabled to send SEMP requests outside of its message vpn.
 * 
 * Copyright 2009-2020 Solace Corporation. All rights reserved.
 */

package com.solace.samples.features.others;

import java.io.StringReader;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

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

public class SempPagingRequests extends SampleApp {
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
	
	public SempPagingRequests() {
	}

	public static void main(String[] args) {
		SempPagingRequests bsub = new SempPagingRequests();
		bsub.run(args);
	}
	
	void run(String[] args) {
		createSession(args);

		try {
			// Create a producer and a consumer and connect to appliance.
			System.out.println("About to connect to appliance. \n[Ensure selected message-vpn is configured as the appliance's management message-vpn.]");
	        session.connect();
			prod = session.getMessageProducer(new PrintingPubCallback());
			cons = session.getMessageConsumer(new PrintingMessageHandler());
			printRouterInfo();
			cons.start();
			System.out.println("Connected!");


			/*
			 * The SEMP request topic is built using the appliance's host name,
			 * supplied as command-line argument.
			 * 
			 * The SEMP request we perform asks to show all queues, and return five
			 * results at a time.
			 */
			// Extract the appliance's management hostname. 
			String routerName = (String) session.getCapability(CapabilityType.PEER_ROUTER_NAME);
			
			final String SEMP_TOPIC_STRING = String.format("#SEMP/%s/SHOW", routerName);
			final Topic SEMP_TOPIC = JCSMPFactory.onlyInstance().createTopic(SEMP_TOPIC_STRING);
			Map<String,String> extraArguments = conf.getArgBag();
			String sempVersion = extraArguments.containsKey("-sv") ? sempVersion = extraArguments.get("-sv") : SEMP_VERSION_TR;
			final String SEMP_SHOW_QUEUES = "<rpc semp-version=\"" + sempVersion + "\"><show><queue><name>*</name><count/><num-elements>5</num-elements></queue></show></rpc>";
			final String MORECOOKIE_START = "<more-cookie>";
			final String MORECOOKIE_END = "</more-cookie>";
			
			System.out.printf("Router name is '%s', SEMP topic address is '%s'\n", routerName, SEMP_TOPIC_STRING);	
			
			// Set up the requestor on an open session to perform request operations.
			Requestor requestor = session.createRequestor();

			/*
			 * We perform requests in a loop. Each new request uses the
			 * more-cookie from the previous response.
			 */
			String next_request = SEMP_SHOW_QUEUES;
			while(next_request != null) {
				BytesXMLMessage requestMsg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
				requestMsg.writeAttachment(next_request.getBytes());
				System.out.println("REQUEST: " + trimXmlForDisplay(next_request, 75)
					+ String.format("(%s bytes)", next_request.getBytes().length));
				BytesXMLMessage replyMsg = requestor.request(requestMsg, 5000, SEMP_TOPIC);

				String replyStr = "";
				if (replyMsg.getAttachmentContentLength() > 0) {
					byte[] bytes = new byte[replyMsg.getAttachmentContentLength()];
					replyMsg.readAttachmentBytes(bytes);
					replyStr = new String(bytes, "UTF-8");
				}
				System.out.println("REPLY: " + trimXmlForDisplay(replyStr, 175)
					+ String.format("(%s bytes)", replyStr.getBytes().length));

				Document doc = loadXmlDoc(replyStr);
				XPathFactory xpfactory = XPathFactory.newInstance();
				XPath xpath = xpfactory.newXPath();

				// Check execute-result using an XPath query.
				String resultCode = (String) xpath.evaluate(
					"string(//execute-result/@code)", 
					doc, 
					XPathConstants.STRING);
				System.out.println("   Result: " + resultCode);
				if (!"ok".equals(resultCode)) {
					throw new Exception(String.format("SEMP response '%s' not OK.", resultCode));
				}

				/*
				 * List queues. We select text nodes under
				 * <queues><queue><name>NAME</name></queue><queues> in the
				 * response.
				 */
				NodeList nl = (NodeList) xpath.evaluate(
					"//show/queue/queues/queue/name", 
					doc, 
					XPathConstants.NODESET);
				for (int i = 0; i < nl.getLength(); i++) {
					Node node = nl.item(i);
					System.out.println("   Queue: " + node.getTextContent());
				}
				
				// Check for more data to request with more-cookie.
				int start_idx = replyStr.indexOf(MORECOOKIE_START);
				if (start_idx >= 0) {
					// more data available
					int end_idx = replyStr.indexOf(MORECOOKIE_END);
					next_request = replyStr.substring(start_idx + MORECOOKIE_START.length(), end_idx);
					System.out.println("Found more-cookie...");
				} else {
					// Abort the loop, no more data.
					next_request = null;
				}
			} // End requestor loop.
			
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
				// At this point the consumer handle is unusable; a new one should be created 
				// by calling cons = session.getMessageConsumer(...) if the application 
				// logic requires the consumer channel to remain open.
			}
			finish(1);
		} catch (Exception ex) {
			System.err.println("Encountered an Exception... " + ex.getMessage());
			ex.printStackTrace();
			finish(1);
		}
	}

	/**
	 * Load a new Document object from an XML string.
	 */
	private static Document loadXmlDoc(String doc) throws Exception {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(false);
		DocumentBuilder builder = factory.newDocumentBuilder();
		return builder.parse(new InputSource(new StringReader(doc)));
	}

	/**
	 * Pretty-print / trim a request or response by putting it all on one line,
	 * and trimming to length.
	 */
	private static String trimXmlForDisplay(String input, int length) {
		String s = input.replaceAll("\r", "");
		s = s.replaceAll("\n", "");
		if (s.length() > length) {
			return s.substring(0, length - 4) + "...";
		}
		return s;
	}

}
