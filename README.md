# solace-samples-java-jcsmp

These are Aaron's updates to the Solace samples. These will be rolled into the main repository soon.
Don't worry about all the warnings when you compile, those are getting changed.

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
    - Proxy (?)
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
