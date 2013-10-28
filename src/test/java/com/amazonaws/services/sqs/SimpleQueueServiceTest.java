package com.amazonaws.services.sqs;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

public class SimpleQueueServiceTest {
	private AmazonSQS sqs;

	@Before
	public void startQueueTest() {
		System.out.println("===========================================");
		String classPath = "AwsCredentials.properties";
		ClasspathPropertiesFileCredentialsProvider propery = new ClasspathPropertiesFileCredentialsProvider(
				classPath);
		System.out.println("haqiqiqi");
		sqs = new AmazonSQSClient(propery);
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		sqs.setRegion(usWest2);
		System.out.println("===========================================");
		System.out.println("Getting Started testing with Amazon SQS");
		System.out.println("===========================================\n");
	}

	@Test
	public void testCreateQueue() {
		String qName = "test_CreateQueue";

		boolean createStatus = createQueue(qName);
		assertTrue(createStatus);

		createStatus = createQueue(qName);
		assertFalse(createStatus);

		String qUrl = getQueueUrl(qName);
		assertNotEquals(qUrl, null);
		boolean status = deleteQueue(qUrl);
		assertTrue(status);

	}

	@Test
	public void testGetQueueUrl() {
		String qName = "testGetQueueUrl";

		boolean status = createQueue(qName);
		assertTrue(status);

		String myQueueUrl = getQueueUrl(qName);
		assertNotNull(myQueueUrl);

		status = deleteQueue(myQueueUrl);
		assertTrue(status);

		myQueueUrl = getQueueUrl(qName);
		assertNull(myQueueUrl);
	}

	@Test
	public void testSetQueueAttribute() {
		String qName = "testSetQueueAttribute";
		boolean status = createQueue(qName);
		assertTrue(status);
		String qUrl = getQueueUrl(qName);
		assertNotNull(qUrl);
		
        boolean success = false;
		
        try {

			Map<String, String> attributes = new HashMap<String, String>();
			String delaySeconds = "DelaySeconds";
			String delayValue = "100";
			attributes.put(delaySeconds, delayValue);
			SetQueueAttributesRequest sqar = new SetQueueAttributesRequest(
					qUrl, attributes);
			sqs.setQueueAttributes(sqar);

			GetQueueAttributesRequest gqar = new GetQueueAttributesRequest(qUrl);
			Collection<String> attributeNames = new ArrayList<String>();
			attributeNames.add(delaySeconds);
			gqar.setAttributeNames(attributeNames );
			GetQueueAttributesResult res = sqs.getQueueAttributes(gqar);
			System.out.println(res.getAttributes().keySet());
			System.out.println(res.getAttributes().values());
			boolean exist = res.getAttributes().containsKey(delaySeconds);
			assertTrue(exist);
			String value = res.getAttributes().get(delaySeconds);
			assertEquals(value, delayValue);
            success = true;
		} catch (AmazonServiceException ase) {
			processASE(ase);
		} catch (AmazonClientException ace) {
			processACE(ace);
		} finally {
			status = deleteQueue(qUrl);
			assertTrue(status);
		}
		assertTrue(success);

	}

	public void testSendAndGetMessage(){
		String qName = "testSendAndGetMessage";
		boolean status = createQueue(qName);
		assertTrue(status);
		String qUrl = getQueueUrl(qName);
		assertNotNull(qName);
		
		boolean success = false;
		try {
			int testNum = 100;
			//send messages 
			for (int loop = 0; loop < testNum; loop ++ ) {
			   String messageInfo = String.format("this is my %d message", loop);
			   SendMessageRequest sendMessageRequest = new SendMessageRequest(qUrl, messageInfo);
			   SendMessageResult sendMessageResult = sqs.sendMessage(sendMessageRequest);
			   System.out.printf("MessageId:%s\n",sendMessageResult.getMessageId());
			   System.out.printf("MessageMD5:%s\n",sendMessageResult.getMD5OfMessageBody());
			 }
			 
			int seqNum = 10;
			for (int loop = 0; loop < testNum/seqNum;loop ++ ){
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(qUrl);
				receiveMessageRequest.setVisibilityTimeout(10);
				receiveMessageRequest.setMaxNumberOfMessages(loop);
				ReceiveMessageResult receive = sqs.receiveMessage(receiveMessageRequest);
				List<Message> messages = receive.getMessages();
				for (Message message : messages) {
					DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(qUrl, message.getReceiptHandle());
					sqs.deleteMessage(deleteMessageRequest);
					System.out.printf("Delete Message %s", message.getBody());
				}
			}
			 
		} catch (AmazonServiceException ase) {
			processASE(ase);
		} catch (AmazonClientException ace) {
			processACE(ace);
		} finally {
			status = deleteQueue(qUrl);
			assertTrue(status);
		}
		
		assertTrue(success);

	}
	
	@Test
	public void testBatch(){
		String qName = "testBatch";
		boolean status = createQueue(qName);
		assertTrue(status);
		String qUrl = getQueueUrl(qName);
		assertNotNull(qName);
		
		boolean success = false;
		try {
			int testNum = 100;
			
			Collection<SendMessageBatchRequestEntry> entries = new ArrayList<SendMessageBatchRequestEntry>();
			//send messages 
			for (int loop = 0; loop < testNum; loop ++ ) {
			   String messageInfo = String.format("this is my %d message", loop);
			   SendMessageBatchRequestEntry e = new SendMessageBatchRequestEntry(String.format("%d", loop), messageInfo);
			   entries.add(e);
			 }
			
			SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(qName);
			
			sendMessageBatchRequest.setEntries(entries);
			sqs.sendMessageBatch(sendMessageBatchRequest );
			 
			int seqNum = 10;
			for (int loop = 0; loop < testNum/seqNum;loop ++ ){
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(qUrl);
				receiveMessageRequest.setVisibilityTimeout(10);
				receiveMessageRequest.setMaxNumberOfMessages(loop);
				ReceiveMessageResult receive = sqs.receiveMessage(receiveMessageRequest);
				List<Message> messages = receive.getMessages();
				for (Message message : messages) {
					DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(qUrl, message.getReceiptHandle());
					sqs.deleteMessage(deleteMessageRequest);
					System.out.printf("Delete Message %s", message.getBody());
				}
			}
			 
		} catch (AmazonServiceException ase) {
			processASE(ase);
		} catch (AmazonClientException ace) {
			processACE(ace);
		} finally {
			status = deleteQueue(qUrl);
			assertTrue(status);
		}
		
		assertTrue(success);

	}
	

	private boolean deleteQueue(String myQueueUrl) {
		// Delete a queue
		try {
			System.out.printf("Delete Queue %s.\n", myQueueUrl);
			sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));
		} catch (AmazonServiceException ase) {
			processASE(ase);
			return false;
		} catch (AmazonClientException ace) {
			processACE(ace);
			return false;
		}
		return true;
	}

	private boolean createQueue(String qName) {
		// Create a queue
		try {
			System.out.printf("Creating a new SQS queue called %s.\n", qName);
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(
					qName);
			String myQueueUrl = sqs.createQueue(createQueueRequest)
					.getQueueUrl();
		} catch (AmazonServiceException ase) {
			processASE(ase);
			return false;
		} catch (AmazonClientException ace) {
			processACE(ace);
			return false;
		}
		return true;
	}

	private String getQueueUrl(String name) {
		try {
			GetQueueUrlRequest getQueueUrl = new GetQueueUrlRequest(name);
			return sqs.getQueueUrl(getQueueUrl).getQueueUrl();
		} catch (AmazonServiceException ase) {
			processASE(ase);
		} catch (AmazonClientException ace) {
			processACE(ace);
		}

		return null;
	}

	private void processASE(AmazonServiceException ase) {
		System.out
				.println("Caught an AmazonServiceException, which means your request made it "
						+ "to Amazon SQS, but was rejected with an error response for some reason.");
		System.out.println("Error Message:    " + ase.getMessage());
		System.out.println("HTTP Status Code: " + ase.getStatusCode());
		System.out.println("AWS Error Code:   " + ase.getErrorCode());
		System.out.println("Error Type:       " + ase.getErrorType());
		System.out.println("Request ID:       " + ase.getRequestId());
	}

	private void processACE(AmazonClientException ace) {
		System.out
				.println("Caught an AmazonClientException, which means the client encountered "
						+ "a serious internal problem while trying to communicate with SQS, such as not "
						+ "being able to access the network.");
		System.out.println("Error Message: " + ace.getMessage());
	}
}
