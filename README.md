# solace-samples-java-jcsmp

These are Aaron's updates to the Solace samples. These will be rolled into the main repository soon.

## TO COMPILE
```
./gradlew assemble
```

## TO RUN
```
cd build/staged
bin/DirectSubscriber <host:port> <message-vpn> <client-username> [password]
bin/DirectPublisher <host:port> <message-vpn> <client-username> [password]
```

## Fundamentals
These will be proper little messaging applications, looping, publishing multiple messages, using subscriptions w/wildcards, and at least logging or echoing errors and API events to the console.
 - HelloWorld (Direct) Pub/Sub
 - Direct
    - Publish-Subscribe
    - Processor
    - Request-Reply
    - Adapter (?)
 - Guaranteed
    - Publish-Subscribe
    - Processor
    - Request-Reply


## Features (broker or API)

These will be mostly the exising samples. Maybe they need to be tweaked? But also considering just using snippets for these, especially API features.

 - Message Replay (API Initiated)
 - Active Flow Consumer Indication
 - Time-To-Live Message Expiry, and Dead Message Queues
 - Last Value Queues
 - Dynamically created/provision Queues

### API
 - How to read create additional Contexts
 - How to do a blocking consumer
 - ?? 

## The Extras

Less common or less important Features?  Perhaps these should be under Features too?

 - Session Transactions
 - Browsing Queues
 - Message Selectors
 - Topic Endpoints
 - PubSub+ Cache

## Patterns

 - Delayed Delivery
 - ?


## API Logging and log4j2

The Solace JCSMP API uses Apache Commons Logging (JCL) (formerly _Jarkarta_ Commons Logging), and is therefore compatible to use with logging frameworks like log4j2.
These samples use log4j2, as you can see inside the `build.gradle` file, as well as in `src/main/resources/`.  Log4j2 easily allows you to configure various outputs such
as console, file, and many others (e.g. JMS!) for all of the JCSMP API logs.

It is best practice to ensure whatever logging implementation you use, that it is configurable at runtime without having to recompile and redeploy.

In the included log configuration file `src/main/resources/log4j2.xml` there are various logging levels that can be configured using JVM system variables.  For example,
use `-Djcsmp_api_log_level=debug` to set the API logging to debug.  If using the Gradle run scripts, use the convinience environment variable `JAVA_OPTS`.  E.g.:
```
export JAVA_OPTS=-Djcsmp_api_log_level=debug
```
 
