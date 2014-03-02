package com.oriordank.endtoend;

import com.oriordank.MapDbPListStore;
import com.oriordank.MapDbPersistenceAdapter;
import com.oriordank.MapDbPersistenceAdapterFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PendingMessageLimitStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.TempUsage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class EndToEndTest {

    public static final String BROKER_URL = "tcp://localhost:61616";
    private BrokerService service;
    private QueueSession session;
    private MessageProducer producer;
    private Queue destination;

    @Before
    public void setup() throws Exception {
        setupService();
        setDestinationPolicy();
        service.start();
        QueueConnection connection = startConnection();
        setupProducer(connection);
    }

    private void setupProducer(QueueConnection connection) throws JMSException {
        session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = session.createQueue("Test");
        producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    }

    private QueueConnection startConnection() throws JMSException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
        QueueConnection connection = connectionFactory.createQueueConnection();
        connection.start();
        return connection;
    }

    private void setupService() throws Exception {
        service = new BrokerService();
        MapDbPersistenceAdapter persistenceAdapter = new MapDbPersistenceAdapter();
        service.setPersistenceAdapter(persistenceAdapter);
        service.setPersistenceFactory(new MapDbPersistenceAdapterFactory());
        service.setPersistent(true);
        service.setTempDataStore(new MapDbPListStore());
        service.addConnector(BROKER_URL);
    }

    private void setDestinationPolicy() {
        PolicyEntry entry = new PolicyEntry();
        entry.setProducerFlowControl(false);
        entry.setMemoryLimit(2048);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(entry);
        service.setDestinationPolicy(policyMap);
    }

    @Test
    public void endToEndTestSimple() throws Exception {
        sendMessage(0);
        MessageConsumer consumer = session.createConsumer(destination);
        Message receivedMessage = consumer.receive();
        assertThatMessageTextIs(receivedMessage, "Test 0");
    }

    private void sendMessage(int index) throws JMSException {
        TextMessage message = session.createTextMessage("Test "+index);
        producer.send(message);
    }

    @Test
    public void volumeTest() throws JMSException {
        service.getSystemUsage().getMemoryUsage().setLimit(2048);
        service.getSystemUsage().getTempUsage().setLimit(2048 * 1024 * 1024);
        for (int i=0; i<20000;i++) {
            sendMessage(i);
        }
        MessageConsumer consumer = session.createConsumer(destination);
        Message receivedMessage;
        int count = 0;
        do {
            receivedMessage = consumer.receiveNoWait();
            if (receivedMessage != null) {
                assertThatMessageTextIs(receivedMessage, "Test " + count);
                count++;
            }
        } while (receivedMessage != null);
        assertThat(count, is(20000));
      
    private void assertThatMessageTextIs(Message message, String text) throws JMSException {
        assertThat(message, instanceOf(TextMessage.class));
        assertThat(((TextMessage)message).getText(), is(text));
    }

    @After
    public void teardown() throws Exception {
        service.stop();
    }
}
