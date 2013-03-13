package com.bma.amazon.sqs;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class MessageLongPollingTest {
	private static final String STOP_MSG = "stop";
	
	private String queueName;
	private HashSet<String> sentMessages;
	private HashSet<String> receivedMessages;
	private BlockingQueue<String> messagesToSend;
	
	private static final int MESSAGE_COUNT = 100;
	private static final int MESSAGE_SIZE = 1024;
	private Random rnd = new Random();
	private InputStream sqsProps;

	
	@Before
	public void setUp() {
		sentMessages = new HashSet<String>();
		receivedMessages = new HashSet<String>();
		messagesToSend = new ArrayBlockingQueue<String>(MESSAGE_COUNT);
		sqsProps = SimpleMessageTest.class.getResourceAsStream("/aws_my.properties");
		
		givenWeHaveAListOfMessages();
		
	}

	private void givenWeHaveAListOfMessages() {
		System.out.println("Creating a list of " + MESSAGE_COUNT + " messages");
		for (int i = 0; i < MESSAGE_COUNT; i++) {
			sentMessages.add(testedMessages());
		}
	}
	
	private String testedMessages() {
		char[] message = new char[MESSAGE_SIZE];
		for (int i = 0; i < MESSAGE_SIZE; i++) {
			message[i] = (char)Math.max(33, rnd.nextInt(127));
		}
		return new String(message);
	}
	
	@Test
	public void verifyPerformance() throws IOException, InterruptedException, ExecutionException {

		System.out.println("Stating test");
		givenWeHaveAQueueName();
		ExecutorService pool = Executors.newCachedThreadPool();
		
		SqsSender sender = giwenWeHaveSender();
		pool.execute(sender);
		
		SqsReceiver receiver = giwenWeHaveReceiver();
		
		long timeSpent = 0;
		for (String msgToSend : sentMessages) {
			Future<String> sendingRes = pool.submit(receiver);
			long sent = System.currentTimeMillis();
			messagesToSend.add(msgToSend);
			String receivedMessage = sendingRes.get();
			long received = System.currentTimeMillis();
			timeSpent += received - sent;
			Assert.assertThat(receivedMessage, CoreMatchers.is(msgToSend));
		}
		messagesToSend.add(STOP_MSG);
		System.out.println("Result: Average latency: " + timeSpent / MESSAGE_COUNT);
	}

	private SqsReceiver giwenWeHaveReceiver() throws IOException {
		AmazonSQSClient sqsSenderClient = createNewSqsClient();
		String queueUrl = sqsSenderClient.createQueue(new CreateQueueRequest(queueName)).getQueueUrl();
		return new SqsReceiver(sqsSenderClient, queueUrl);
	}

	private AmazonSQSClient createNewSqsClient() throws IOException {
		sqsProps = SimpleMessageTest.class.getResourceAsStream("/aws_my.properties");
		AmazonSQSClient sqsSenderClient = new AmazonSQSClient(new PropertiesCredentials(sqsProps));
		sqsSenderClient.setEndpoint("sqs.eu-west-1.amazonaws.com");
		return sqsSenderClient;
	}

	private SqsSender giwenWeHaveSender() throws IOException {
		AmazonSQSClient sqsSenderClient = createNewSqsClient();
		String queueUrl = sqsSenderClient.createQueue(new CreateQueueRequest(queueName)).getQueueUrl();
		return new SqsSender(sqsSenderClient, queueUrl, messagesToSend);
	}
	
	private void givenWeHaveAQueueName() {
		queueName = "test_queue_" + System.currentTimeMillis();
	}
	
	private static class SqsSender implements Runnable {
		
		private AmazonSQSClient sqsClient;
		private BlockingQueue<String> messagesToSend;
		private String queueUrl;
		
		public SqsSender(AmazonSQSClient sqsClient, String queueUrl,
				BlockingQueue<String> messagesToSend) {
			this.sqsClient = sqsClient;
			this.queueUrl = queueUrl;
			this.messagesToSend = messagesToSend;
		}

		public void run() {
			try {
				startSenging();
			} catch (Exception e) {
				System.err.println(e);
			}
		}

		private void startSenging() throws AmazonServiceException, AmazonClientException, InterruptedException {
			String message;
			while (!(message = messagesToSend.take()).equals(STOP_MSG)) {
				sqsClient.sendMessage(new SendMessageRequest(queueUrl, message));
				//System.out.println("Sent message: " + message);
			}
			System.out.println("Stop sending");
		}
	}
	
	private static class SqsReceiver implements Callable<String> {
		private AmazonSQSClient sqsClient;
		private String queueUrl;
		
		public SqsReceiver(AmazonSQSClient sqsClient, String queueUrl) {
			this.sqsClient = sqsClient;
			this.queueUrl = queueUrl;
		}

		public String call() throws Exception {
			return receiveMessage();
		}

		private String receiveMessage() {
			ReceiveMessageRequest request = 
				new ReceiveMessageRequest().withQueueUrl(queueUrl).withWaitTimeSeconds(5);
				//new ReceiveMessageRequest().withQueueUrl(queueUrl);
			ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(request);
			if (receiveMessageResult.getMessages().size() == 1) {
				String message = receiveMessageResult.getMessages().get(0).getBody();
				//System.out.println("Received message: " + message);
				return message;
			} else if (receiveMessageResult.getMessages().size() > 1) {
				System.err.println("Messages in queue is more then 1");
			} else {
				System.err.println("Queue is empty");
			}
			return null;
		}
		
	}
}


