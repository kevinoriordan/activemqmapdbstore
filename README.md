activemqmapdbstore
==================

ActiveMQ MapDB Off heap temp storage

Usage
========

XML configuration:
--------

Include the namespace declaration under opening beans tag in active.xml:

```xml
<beans
  xmlns="http://www.springframework.org/schema/beans"
  xmlns:amq="http://activemq.apache.org/schema/core"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:k="http://oriordank.com/schema/mapdbstore"
  xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
  http://activemq.apache.org/schema/core http://activemq.apache.org/schema/core/activemq-core.xsd
  http://oriordank.com/schema/mapdbstore http://oriordank.com/schema/mapdbstore/mapdbstore.xsd">
```

Set persistent mode to true on broker:

```xml
    <broker xmlns="http://activemq.apache.org/schema/core" brokerName="instance1" useShutdownHook="true" persistent="true" advisorySupport="true" schedulerSupport="false">
```

Install forcePersistentMode plugin to make sure all message producers use non-persistent delivery (combination of persistent true on broker and peristent false on producer means temp store is used).

```xml
        <plugins>
                <forcePersistencyModeBrokerPlugin peristenceFlag="false"/>
        </plugins>
```

Under system usage, give enough memory to tempStore:

```xml
          <systemUsage>
            <systemUsage sendFailIfNoSpace="true">
                <memoryUsage>
                    <memoryUsage limit="20mb"/>
                </memoryUsage>
                <storeUsage>
                    <storeUsage limit="32mb"/>
                </storeUsage>
                <tempUsage>
                    <tempUsage limit="12 gb"/>
                </tempUsage>
            </systemUsage>
        </systemUsage>
```

Install MapDB PList Store:

```xml
        <tempDataStore>
                <k:mapDbTempStore/>
        </tempDataStore>
```

Java broker configuration:
--------

```java
        BrokerService broker = new BrokerService();
        broker.setPersistent(true);
        broker.setTempDataStore(new MapDbPListStore());
        broker.addConnector(BROKER_URL);
        
        Session session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue("Test");
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);


