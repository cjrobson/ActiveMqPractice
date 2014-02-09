package com.robsonc.producer;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

public class ActiveMqApp {


	public static void thread(Runnable runnable, boolean daemon) {
		Thread brokerThread = new Thread(runnable);
		brokerThread.setDaemon(daemon);
		brokerThread.start();
	}
	
	public static class HelloWorldProducer implements Runnable {

		
		@Override
		public void run() {
			try {
				// Create a connection factory
				ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
						"vm://localhost");

				// Create a connection
				Connection connection = connectionFactory.createConnection();
				connection.start();

				// Create a session
				Session session = connection.createSession(false,
						Session.AUTO_ACKNOWLEDGE);

				// Create the destination (Topic or Queue)
				Destination destination = session.createQueue("TEST.FOO");

				// Create a MessageProducer from the session to the topic or
				// queue
				MessageProducer producer = session.createProducer(destination);
				producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

				// Create messages
				String text = "Hello world! From: "
						+ Thread.currentThread().getName() + " : "
						+ this.hashCode();
				TextMessage message = session.createTextMessage(text);

				// Tell the producer to send the message
				System.out.println("Send the message: " + message.hashCode()
						+ " : " + Thread.currentThread().getName());
				producer.send(message);

				// Clean up
				session.close();
				connection.close();

			} catch (Exception e) {
				System.out.println("Caught: " + e);
				e.printStackTrace();
			}

		}
	}

	public static class HelloWorldConsumer implements Runnable, ExceptionListener {
		public void run() {
			try {
//				Create a ConnectionFactory
				ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
				
//				Create a connection
				Connection connection = connectionFactory.createConnection();
				connection.start();
				
				connection.setExceptionListener(this);
				
//				Create a session
				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				
//				Create the destination (topic or queue)
				Destination destination = session.createQueue("TEST.FOO");
				
//				Create a MessageConsumer from the session to the topic or queue
				MessageConsumer consumer = session.createConsumer(destination);
				
//				Wait for a message
				Message message = consumer.receive(1000);
				
				if (message instanceof TextMessage) {
					TextMessage textMessage = (TextMessage)message;
					String text = textMessage.getText();
					System.out.println("Received: " + text);
				} else {
					System.out.println("Received: " + message);
				}
					
				consumer.close();
				session.close();
				connection.close();
			} catch (Exception e) {
				System.out.println("Caught: " + e);
				e.printStackTrace();
			}
		}
		@Override
		public synchronized void onException(JMSException ex) {
			System.out.println("JMS Exception occurred. Shutting down client.");
		}
	}
	
	public static void main(String[] args) throws Exception {
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldConsumer(), false);
		Thread.sleep(1000);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
		Thread.sleep(1000);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldProducer(), false);
		Thread.sleep(1000);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
	}
}
