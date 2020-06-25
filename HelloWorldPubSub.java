


package com.solace.samples.aaron;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class HelloWorldPubSub {
    
    private static final CountDownLatch shutdownGate = new CountDownLatch(1);
    private static final String topicPrefix = "hello/world";
    private static volatile String topicLevel3 = "default";

    public static void main(String... args) throws JCSMPException, IOException, InterruptedException {
        // Check command line arguments
        if (args.length < 3) {
            System.out.printf("Usage: HelloWorldPubSub <host:port> <client-username> <message-vpn> [client-password]%n%n");
            System.exit(-1);
        }
        System.out.println("HellowWorldPubSub initializing...");
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);     // host:port
        properties.setProperty(JCSMPProperties.VPN_NAME,  args[1]);  // message-vpn
        properties.setProperty(JCSMPProperties.USERNAME, args[2]);   // client-username
        if (args.length > 3) {
            properties.setProperty(JCSMPProperties.PASSWORD, args[3]);  // client-password
        }
        properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);  // re-subscribe after reconnecting
        JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
        channelProps.setReconnectRetries(5);
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES,channelProps);
        final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties,null,new SessionEventHandler() {
			@Override
			public void handleEvent(SessionEventArgs event) {
                System.out.printf("))) Session Event: %s%n",event.getEvent());
			}
		});
        
        System.out.print("Enter your name or a unique word: ");
        topicLevel3 = new BufferedReader(new InputStreamReader(System.in)).readLine().replaceAll(" ", "_");

        // CONSUMER STUFF FIRST ///////////////////////////////////////////////////////////////////////////////////////////////
        // Anonymous inner-class for MessageListener, this demonstrates the async threaded message callback */
        final XMLMessageConsumer consumer = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage msg) {
                // could be 4 different message types: 3 SMF ones (Text, Map, Stream) and just plain binary
                   System.out.printf("vvv RECEIVED MSG vvv%n%s%n",msg.dump());
            }

            @Override
            public void onException(JCSMPException e) {  // uh oh!
                System.out.printf("### MessageListener's onException(): %s%n",e);
                if (e instanceof JCSMPTransportException) {  // unrecoverable
                    shutdownGate.countDown();
                }
            }
        });
        
        // PRODUCER CALLBACKS NEXT ////////////////////////////////////////////////////////////////////////////////////
        // Anonymous inner-class for handling publishing events
        XMLMessageProducer producer = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
            // these first 2 are never called, they have been replaced by the "Ex" versions
            @Override public void responseReceived(String messageID) { }
            @Override public void handleError(String messageID, JCSMPException cause, long timestamp) { }
            @Override public void responseReceivedEx(Object key) { }  // only used in Guaranteed/Persistent publishing application
            
            @Override
            public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
                System.out.printf("### Producer handleErrorEx() callback: %s%n",cause);
                if (cause instanceof JCSMPTransportException) {  // unrecoverable
                    shutdownGate.countDown();
                }
            }
        }, null);  // null is the ProducerEvent handler... don't need it in this simple application

        // NOW START APPLICATION /////////////////////////////////////////////////////////////////////////////////////////////////
        session.connect();
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic(topicPrefix+"/>"));
        //session.addSubscription(JCSMPFactory.onlyInstance().createTopic("control/*"));  // to receive "quit" commands
        consumer.start();
        final AtomicInteger msgSeqNum = new AtomicInteger();

        Runnable publisherRunnable = () -> {
            BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
            try {
                while (true) {
                    message.setSequenceNumber(msgSeqNum.incrementAndGet());
                    message.setApplicationMessageId(UUID.randomUUID().toString());
                    String payloadText = String.format("message='%s'; time='%s'; sender='%s'; seq=%d",
                    		"Hello World!!!",LocalDateTime.now(),topicLevel3,msgSeqNum.get());
                    message.setData(payloadText.getBytes(Charset.forName("UTF-8")));
                    Topic t = JCSMPFactory.onlyInstance().createTopic(
                            String.format("%s/%s/%d",topicPrefix,topicLevel3,msgSeqNum.get()));
                    System.out.printf(">>> Calling send() for #%d on %s%n",msgSeqNum.get(),t.getName());
                    producer.send(message,t);
                    message.reset();  // reuse this message on the next loop, to avoid having to recreate it
                    Thread.sleep(5000);
                }
            } catch (JCSMPException e) {
                System.out.printf("### publisherRunnable publish() exception: %s%n",e);
                if (e instanceof JCSMPTransportException) {  // unrecoverable
                    shutdownGate.countDown();
                }
            } catch (InterruptedException e) {
                // IGNORE... probably getting shut down
            } finally {
                System.out.println("### publisherRunnable exiting");
            }
        };
        Thread publisherThread = new Thread(publisherRunnable,"Publisher Thread");
        publisherThread.setDaemon(true);
        publisherThread.start();
        
        System.out.println("Connected, and running.");
        shutdownGate.await();
        System.out.println("Main thread quitting.");
        session.closeSession();  // quit right away
        Thread.sleep(5000);
    }
}
