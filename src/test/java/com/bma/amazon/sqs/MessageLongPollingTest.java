package com.bma.amazon.sqs;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class MessageLongPollingTest {
	private static final String TOKYO_QUEUE_ENDPOINT = "sqs.ap-northeast-1.amazonaws.com";
	private static final String IERLAND_QUEUE_ENDPOINT = "sqs.eu-west-1.amazonaws.com";
	private static final String Virginia_QUEUE_ENDPOINT = "sqs.us-east-1.amazonaws.com";
	private static final String Oregon_QUEUE_ENDPOINT = "sqs.us-west-2.amazonaws.com";
	private static final String California_QUEUE_ENDPOINT = "sqs.us-west-1.amazonaws.com";
	private static final String Singapore_QUEUE_ENDPOINT = "sqs.ap-southeast-1.amazonaws.com";
	private static final String Sydney_QUEUE_ENDPOINT = "sqs.ap-southeast-2.amazonaws.com";
	private static final String Sao_Paulo_QUEUE_ENDPOINT = "sqs.sa-east-1.amazonaws.com";

	private String queueEndpoint = IERLAND_QUEUE_ENDPOINT;

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
		//doTest(TOKYO_QUEUE_ENDPOINT);
		//doTest(IERLAND_QUEUE_ENDPOINT);
		//doTest(Virginia_QUEUE_ENDPOINT);
		//doTest(Oregon_QUEUE_ENDPOINT);
		//doTest(California_QUEUE_ENDPOINT);
		doTest(Singapore_QUEUE_ENDPOINT);
		doTest(Sydney_QUEUE_ENDPOINT);
		doTest(Sao_Paulo_QUEUE_ENDPOINT);
	}

	private void doTest(String endpoint) throws IOException, InterruptedException,
			ExecutionException {
		queueEndpoint = endpoint;
		System.out.println("Stating test for endpoint: " + queueEndpoint);
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
		
		List<MessageTimes> senderTimes = sender.messageTimes;
		List<MessageTimes> receiverTimes = receiver.messageTimes;

		printPerformanceMeasurements(senderTimes, receiverTimes);
	}

	private void printPerformanceMeasurements(List<MessageTimes> senderTimes,
			List<MessageTimes> receiverTimes) {
		HashMap<String, MessageTimes> map = new HashMap<String, MessageTimes>();
		for(MessageTimes times : senderTimes) {
			map.put(times.messageText, times);
		}
	
		for(MessageTimes times : receiverTimes) {
			MessageTimes sentTimes = map.get(times.messageText);
			sentTimes.copyFromReceiver(times);
			map.put(sentTimes.messageText, sentTimes);
		}
		
		long latency = 0;
		long latencyWithDelete = 0;
		long sendTime = 0;
		long deleteTime = 0;
		long receiveTime = 0;
		for (MessageTimes times : map.values()) {
			latency += times.messageReceived -  times.messageStartSending; 
			latencyWithDelete += times.messageDeleted - times.messageStartSending;
			sendTime += times.messageSent - times.messageStartSending; 
			deleteTime += times.messageDeleted - times.messageReceived;
			receiveTime += times.messageReceived -  times.messageSent; 
		}
		
		System.out.print("Latency: " + latency/MESSAGE_COUNT);
		System.out.print(", Latency With Delete: " + latencyWithDelete/MESSAGE_COUNT);
		System.out.print(", Delete Time: " + deleteTime/MESSAGE_COUNT);
		System.out.print(", Send Time: " + sendTime/MESSAGE_COUNT);
		System.out.print(", Receive Time: " + receiveTime/MESSAGE_COUNT);
		System.out.println();
	}

	private SqsReceiver giwenWeHaveReceiver() throws IOException {
		AmazonSQSClient sqsSenderClient = createNewSqsClient();
		String queueUrl = sqsSenderClient.createQueue(new CreateQueueRequest(queueName)).getQueueUrl();
		return new SqsReceiver(sqsSenderClient, queueUrl);
	}

	private AmazonSQSClient createNewSqsClient() throws IOException {
		sqsProps = SimpleMessageTest.class.getResourceAsStream("/aws_my.properties");
		AmazonSQSClient sqsSenderClient = new AmazonSQSClient(new PropertiesCredentials(sqsProps));
		sqsSenderClient.setEndpoint(queueEndpoint);
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
		private long timeSpentToSend = 0;
		private List<MessageTimes> messageTimes = new ArrayList<MessageLongPollingTest.MessageTimes>();
		
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
				MessageTimes times = MessageTimes.startSending(message);
				messageTimes.add(times);
				sqsClient.sendMessage(new SendMessageRequest(queueUrl, message));
				times.sent();
				//System.out.println("Sent message: " + message);
			}
			System.out.println("Stop sending");
		}

		public long getTimeSpentToSend() {
			return timeSpentToSend;
		}
		
	}
	
	private static class SqsReceiver implements Callable<String> {
		private AmazonSQSClient sqsClient;
		private String queueUrl;
		private List<MessageTimes> messageTimes = new ArrayList<MessageLongPollingTest.MessageTimes>();
		
		
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
			
			ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(request);
			
			if (receiveMessageResult.getMessages().size() == 1) {
				Message message = receiveMessageResult.getMessages().get(0);
				String messageText = message.getBody();
				MessageTimes times = MessageTimes.received(messageText);
				
				messageTimes.add(times);
				//System.out.println("Received message: " + messageText);
				
				deleteMessage(message);
				times.deleted();
				
				return messageText;
			} else if (receiveMessageResult.getMessages().size() > 1) {
				System.err.println("Messages in queue is more then 1");
			} else {
				System.err.println("Queue is empty");
			}
			return null;
		}

		private void deleteMessage(Message message) {
			sqsClient.deleteMessage(new DeleteMessageRequest().withQueueUrl(queueUrl).withReceiptHandle(message.getReceiptHandle()));
		}
		
	}
	
	
	private static class MessageTimes {
		public String messageText;
		long messageStartSending;
		long messageSent;
		long messageReceived;
		long messageDeleted;
		
		public static MessageTimes startSending(String msg) {
			MessageTimes times = new MessageTimes();
			times.messageText = msg;
			times.messageStartSending = System.currentTimeMillis();
			return times;
		}
		public void copyFromReceiver(MessageTimes times) {
			this.messageDeleted = times.messageDeleted;
			this.messageReceived = times.messageReceived;
		}
		public void deleted() {
			messageDeleted = System.currentTimeMillis();
			
		}
		public static MessageTimes received(String msg) {
			MessageTimes times = new MessageTimes();
			times.messageReceived = System.currentTimeMillis();
			times.messageText = msg;
			return times;
			
		}
		public void sent() {
			messageSent =  System.currentTimeMillis();
		}
	}
}


