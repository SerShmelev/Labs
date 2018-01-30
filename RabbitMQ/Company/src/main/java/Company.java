import com.rabbitmq.client.*;
import java.util.Scanner;

public class Company {
	private static final String REQUEST_QUEUE = "company_queue";

	public static void main(String[] args) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(REQUEST_QUEUE, true, false, false, null);

		while (true) {
			Scanner in = new Scanner(System.in);

			String request = in.nextLine();

			channel.basicPublish("", REQUEST_QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN, request.getBytes("UTF-8"));

			System.out.println("Request : " + request);
		}

	}
}
