import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import java.util.Arrays;
import java.util.Scanner;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class Client {

	private Connection connection;
	private static Channel channel;
	private static String requestQueueName = "client_queue";
	private static String replyQueueName;

	public Client() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");

		connection = factory.newConnection();
		channel = connection.createChannel();

		replyQueueName = channel.queueDeclare().getQueue();
	}

	public static String call() throws IOException, InterruptedException {
		final String corrId = UUID.randomUUID().toString();

		Scanner in = new Scanner(System.in);
		System.out.print("Ask: ");
		String message = in.nextLine();

		AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().correlationId(corrId).replyTo(replyQueueName)
				.build();
		System.out.println("Reply queueName: " + replyQueueName);

		
		channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));
		final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);
		channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				if (properties.getCorrelationId().equals(corrId)) {	
					response.offer(new String(body, "UTF-8"));
				}	
			}
		});
		return response.take();
	}

	public void close() throws IOException {
		connection.close();
	}

	public static void main(String[] args) throws IOException {
		Client client = null;
		String response = null;
		try {
			client = new Client();
			response = call();
			System.out.println("Answer: " + response);			
			String[] rec = response.split("$");
			System.out.println(Arrays.toString(rec));
		} catch (IOException | TimeoutException | InterruptedException e) {
			e.printStackTrace();
		} finally {
			if (client != null) {
				try {
					client.close();
				} catch (IOException _ignore) {
				}
			}
		}
	}
}