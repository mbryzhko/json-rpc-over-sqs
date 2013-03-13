package com.bma.amazon.sqs;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SimpleMessageTest {
	
	private AmazonSQSClient sqsClient;
	private AmazonSQSClient sqsSerive;
	private String queueName;
	private String queueUrl;

	@After
	public void tearDown() {
		if (queueUrl != null) { 
			sqsClient.deleteQueue(new DeleteQueueRequest(queueUrl));
		}
	}
	
	@Test
	public void verifyThatQueueCanBeCreated() throws IOException {
		givenWeHaveSqsClient();
		
		whenCreateQueue();
		
		System.out.println("Created queue url " + queueUrl);
	}

	private void whenCreateQueue() {
		queueName = "test_queue_" + System.currentTimeMillis();
		CreateQueueResult createQueueResult = sqsClient.createQueue(new CreateQueueRequest(queueName));
		queueUrl = createQueueResult.getQueueUrl();
	}
	
	@Test
	public void verifyThatMessageCanBeSent() throws Exception {
		givenWeHaveSqsClient();
		givenWeHaveSqsService();
		whenCreateQueue();
		
		whenSendAMessage("Hello SQS");
		
		thenMessageShouldBeReceived("Hello SQS");
	}
	
	@Test
	public void verifyThatMessageCanBeDeleted() throws IOException {
		givenWeHaveSqsClient();
		givenWeHaveSqsService();
		whenCreateQueue();
		
		whenSendAMessage("Hello SQS");
		
		thenMessageIsReceivedAndDeleted();
		thenNoMessageInTheQueue();
		
	}
	
	@Test
	public void verifyThatQueueCanBeDeleted() throws IOException {
		givenWeHaveSqsClient();
		List<String> queueUrls = sqsClient.listQueues().getQueueUrls();
		for (String string : queueUrls) {
			sqsClient.deleteQueue(new DeleteQueueRequest().withQueueUrl(string));
		}
	}

	private void thenNoMessageInTheQueue() {
		List<Message> messages = receiveMessage();
		Assert.assertThat(messages.size(), is(0));
	}

	private void thenMessageIsReceivedAndDeleted() {
		List<Message> messages = receiveMessage();
		for (Message message : messages) {
			deleteMessageFromAQueue(message);
		}
	}

	private void thenMessageShouldBeReceived(String expectedText) {
		List<Message> messages = receiveMessage();
		
		assertThat(messages.size(), is(1));
		Message receivedMessage = messages.get(0);
		assertThat(receivedMessage.getBody(), is(expectedText));
		
		deleteMessageFromAQueue(receivedMessage);
	}

	private void deleteMessageFromAQueue(Message message) {
		System.out.println("Deleting message " + message);
		sqsSerive.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
	}

	private List<Message> receiveMessage() {
		ReceiveMessageResult messageResult = sqsSerive.receiveMessage(new ReceiveMessageRequest(queueUrl));
		System.out.println("Received Message: " + messageResult.toString());
		List<Message> messages = messageResult.getMessages();
		return messages;
	}

	private void givenWeHaveSqsService() throws IOException {
		InputStream properties = SimpleMessageTest.class.getResourceAsStream("/aws.properties");
		sqsSerive = new AmazonSQSClient(new PropertiesCredentials(properties));
		sqsSerive.setEndpoint("sqs.eu-west-1.amazonaws.com");
	}

	private void whenSendAMessage(String text) {
		System.out.println("Sending message: " + text + " to queue: " + queueUrl);
		sqsClient.sendMessage(new SendMessageRequest(queueUrl, text));
	}

	private void givenWeHaveSqsClient() throws IOException {
		InputStream properties = SimpleMessageTest.class.getResourceAsStream("/aws_my.properties");
		sqsClient = new AmazonSQSClient(new PropertiesCredentials(properties));
		sqsClient.setEndpoint("sqs.eu-west-1.amazonaws.com");
	}
}
