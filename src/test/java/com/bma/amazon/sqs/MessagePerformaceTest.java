package com.bma.amazon.sqs;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class MessagePerformaceTest {
	
	private String queueName;
	
	private Set<String> sentMessages;
	private Set<String> receivedMessages;
	private BlockingQueue<String> messagesToSend;
	private AtomicInteger sendersWhichStillRunning = new AtomicInteger(0);
	
	private static final int SIZE = 100;
	private static final int MESSAGE_SIZE = 1024;
	private Random rnd = new Random();

	private List<SqsClient> senders;

	private ArrayList<SqsClient> receivers;
	
	
	@Before
	public void setUp() {
		sentMessages = new HashSet<String>();
		receivedMessages = new HashSet<String>();
		messagesToSend = new ArrayBlockingQueue<String>(SIZE);
	
		givenWeHaveAListOfMessages();
		
		messagesToSend.addAll(sentMessages);
	}
	
	private void givenWeHaveAListOfMessages() {
		System.out.println("Creating a list of " + SIZE + " messages");
		for (int i = 0; i < SIZE; i++) {
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

	@After
	public void tearDown() {
		
	}
	
	@Test
	public void verifyPerfomance() throws InterruptedException, IOException, ExecutionException {
		givenWeHaveAQueueName();
		givenWeHaveWorkers();

		ExecutorService pool = Executors.newCachedThreadPool();
		whenSendMessages(pool);
		whenReceiveMessages(pool);
		
		for (SqsClient sqsClient : receivers) {
			if (sqsClient.isReceive()) {
				receivedMessages.addAll(sqsClient.getReceived());
			}
		}
		Assert.assertThat(receivedMessages.size(), CoreMatchers.is(sentMessages.size()));
			
		
	}

	private void whenReceiveMessages(ExecutorService pool)
			throws InterruptedException, ExecutionException {
		long duration;
		duration = System.currentTimeMillis();
		List<Future<Long>> resultsReceivers = pool.invokeAll(receivers);
		duration = System.currentTimeMillis() - duration;
		
		long sum = 0;
		for (Future<Long> future : resultsReceivers) {
			sum += future.get();
		}
		System.out.println("Receiver: duration: " + duration + ", total spent: " + sum + ", avg: " + sum / SIZE);
	}

	private void whenSendMessages(ExecutorService pool)
			throws InterruptedException, ExecutionException {
		long duration = System.currentTimeMillis();
		List<Future<Long>> resultSenders = pool.invokeAll(senders);
		duration = System.currentTimeMillis() - duration;
		
		long sum = 0;
		for (Future<Long> future : resultSenders) {
			sum += future.get();
		}
		System.out.println("Senders: duration: " + duration + ", total spent: " + sum + ", avg: " + sum / SIZE);
	}

	private void givenWeHaveWorkers() throws IOException {
		senders = new ArrayList<SqsClient>();
		senders.add(new SqsClient(false));
		//senders.add(new SqsClient(false));
		//senders.add(new SqsClient(false));
		
		receivers = new ArrayList<SqsClient>();
		receivers.add(new SqsClient(true));
		//receivers.add(new SqsClient(true));
		//receivers.add(new SqsClient(true));
		//receivers.add(new SqsClient(true));
	}
	
	private void givenWeHaveAQueueName() {
		queueName = "test_queue_" + System.currentTimeMillis();
	}

	public class SqsClient implements Callable<Long> {
		private boolean receive;
		private AmazonSQSClient sqsClient;
		private String queueUrl;
		private List<String> receivedMessages;
		private long startTime;
		
		public SqsClient(boolean receive) throws IOException {
			this.receive = receive;
			if (!receive) {
				sendersWhichStillRunning.intValue();
			}
			receivedMessages = new LinkedList<String>();
			createSqsClient();
			resolveQueueUrl();
		}

		private void resolveQueueUrl() {
			System.out.println("Creating/Resolving queue: " + queueName);
			queueUrl = sqsClient.createQueue(new CreateQueueRequest().withQueueName(queueName)).getQueueUrl();
		}

		private void createSqsClient() throws IOException {
			InputStream properties = SimpleMessageTest.class.getResourceAsStream("/aws_my.properties");
			sqsClient = new AmazonSQSClient(new PropertiesCredentials(properties));
			sqsClient.setEndpoint("sqs.eu-west-1.amazonaws.com");
		}

		public Long call() throws Exception {
			startTime = System.currentTimeMillis();
			if (receive) {
				System.out.println("Start Receiving");
				List<Message> messages = null;
				ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest().withQueueUrl(queueUrl).withWaitTimeSeconds(20);
				while (!(messages = sqsClient.receiveMessage(receiveRequest).getMessages()).isEmpty()) {
					if (!messages.isEmpty()) {
						for (Message message : messages) {
							registerReceivedMessage(message);
							deleteMessageFromAQueue(message);
						}
					} else {
						System.out.println("Received empty messsage");
					}
				}
				System.out.println("Stop reading");
			} 
			else {
				System.out.println("Start Sending");
				String nextMessageText = null;
				while((nextMessageText = messagesToSend.poll()) != null) {
					//System.out.println("Sending message: " + nextMessageText);
					SendMessageRequest message = new SendMessageRequest().withMessageBody(nextMessageText).withQueueUrl(queueUrl);
					sqsClient.sendMessage(message);
				}
				sendersWhichStillRunning.decrementAndGet();
			}
			return System.currentTimeMillis() - startTime;
		}

		private void registerReceivedMessage(Message message) {
			receivedMessages.add(message.getBody());
			//System.out.println("Received message: " + message.getBody());
		}

		private void deleteMessageFromAQueue(Message message) {
			sqsClient.deleteMessage(new DeleteMessageRequest().withQueueUrl(queueUrl).withReceiptHandle(message.getReceiptHandle()));
		}

		public boolean isReceive() {
			return receive;
		}

		public List<String> getReceived() {
			return receivedMessages;
		}
	}
}
