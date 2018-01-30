import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Aggregator {

	private static final String COMPANY_QUEUE = "company_queue";
	private static final String CLIENT_QUEUE = "client_queue";

	public static void insertDB(String request) {
		java.sql.Connection connectionDB = null;
		String[] rec = request.split(" ");
		String url = "jdbc:postgresql://127.0.0.1:5432/lab8";
		String name = "sergey";
		String password = "qwe123";

		String company = rec[0];
		String product = rec[1];
		float price = Float.parseFloat(rec[2]);

		try {
			Class.forName("org.postgresql.Driver");
			System.out.println("Драйвер подключен");
			connectionDB = DriverManager.getConnection(url, name, password);
			System.out.println("Соединение установлено");

			PreparedStatement preparedStatement = null;
			preparedStatement = connectionDB.prepareStatement(
					"INSERT INTO products " + "VALUES ('" + product + "', '" + price + "', '" + company + "');");

			int result = preparedStatement.executeUpdate();
			System.out.println("Vot: " + result);
		} catch (Exception e) {
			Logger.getLogger(Aggregator.class.getName()).log(Level.SEVERE, null, e);
		} finally {
			if (connectionDB != null) {
				try {
					connectionDB.close();
				} catch (SQLException e) {
					Logger.getLogger(Aggregator.class.getName()).log(Level.SEVERE, null, e);
				}
			}
		}
		System.out.println(Arrays.toString(rec));

	}

	public static String doSQL(String request) {
		String[] rec = request.split(" ");
		System.out.println(Arrays.toString(rec));
		String product = null;
		String opt = null;

		String sql = null;
		if (rec.length == 1) {
			product = rec[0];
			sql = "SELECT * FROM products WHERE product = '" + product + "';";
		} else {
			opt = rec[1];
			product = rec[0];

			sql = "SELECT * FROM products WHERE price = (SELECT " + opt + "(price) FROM products WHERE product = '"
					+ product + "');";
		}
		return sql;
	}

	public static String selectDB(String request) {
		java.sql.Connection connectionDB = null;
		String url = "jdbc:postgresql://127.0.0.1:5432/lab8";
		String name = "sergey";
		String password = "qwe123";
		Statement stmt = null;
		String sql = null;
		String Answer = "";

		try {
			Class.forName("org.postgresql.Driver");
			System.out.println("Драйвер подключен");
			connectionDB = DriverManager.getConnection(url, name, password);
			System.out.println("Соединение установлено");
			stmt = connectionDB.createStatement();
			System.out.println("Делаю SQL");
			sql = doSQL(request);
			System.out.println("SQL: " + sql);
			ResultSet result = stmt.executeQuery(sql);
			while (result.next()) {
				String product = result.getString("product");
				float price = result.getFloat("price");
				String company = result.getString("company");

				Answer = Answer + product + " | " + price + " | " + company + "$";
			}
			result.close();
			stmt.close();
			connectionDB.close();
		} catch (Exception e) {
			Logger.getLogger(Aggregator.class.getName()).log(Level.SEVERE, null, e);
		}

		return Answer;
	}

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		final Connection companyConnection = factory.newConnection();
		final Channel companyChannel = companyConnection.createChannel();

		companyChannel.queueDeclare(COMPANY_QUEUE, true, false, false, null);
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		companyChannel.basicQos(1);

		Connection clientConnection = null;
		clientConnection = factory.newConnection();
		final Channel clientChannel = clientConnection.createChannel();
		clientChannel.queueDeclare(CLIENT_QUEUE, true, false, false, null);

		clientChannel.basicQos(1);
		System.out.println(" [x] Awaiting RPC requests");

		try {
			Consumer clientConsumer = new DefaultConsumer(clientChannel) {
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
						byte[] body) throws IOException {
					AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
							.correlationId(properties.getCorrelationId()).build();
					System.out.println("Создаю ответ");
					String response = "";
					try {
						String message = new String(body, "UTF-8");
						response = selectDB(message);

					} catch (RuntimeException e) {
						System.out.println("[.]" + e.toString());
					} finally {
						clientChannel.basicPublish("", properties.getReplyTo(), replyProps, response.getBytes("UTF-8"));
						clientChannel.basicAck(envelope.getDeliveryTag(), false);
						synchronized (this) {
							this.notify();
						}
					}
				}
			};

			clientChannel.basicConsume(CLIENT_QUEUE, false, clientConsumer);

			while (true) {
				synchronized (clientConsumer) {
					try {
						clientConsumer.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (clientConnection != null) {
				try {
					clientConnection.close();
				} catch (IOException _ignore) {
				}
			}
		}

		final Consumer companyConsumer = new DefaultConsumer(companyChannel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				String message = new String(body, "UTF-8");
				try {
					insertDB(message);
				} finally {
					companyChannel.basicAck(envelope.getDeliveryTag(), false);
				}
			}
		};
		companyChannel.basicConsume(COMPANY_QUEUE, false, companyConsumer);

	}
}

//
// public static void main(String[] argv) throws Exception {
// ConnectionFactory factory = new ConnectionFactory();
// factory.setHost("localhost");
// final Connection companyConnect = factory.newConnection();
// final Channel companyChannel = companyConnect.createChannel();
//
// companyChannel.queueDeclare(COMPANY_QUEUE, true, false, false, null);
// companyChannel.basicQos(1);
//
//
// System.out.println(" [*] Waiting for messages");
//
//
// final Consumer consumer = new DefaultConsumer(companyChannel) {
// @Override
// public void handleDelivery(String consumerTag, Envelope envelope,
// AMQP.BasicProperties properties,
// byte[] body) throws IOException {
// String message = new String(body, "UTF-8");
//
// System.out.println(" [x] Received '" + message + "'");
// insertDB(message);
// }
// };
// companyChannel.basicConsume(COMPANY_QUEUE, false, consumer);
// }
//
// }
// private static final String CLIENT_QUEUE = "client_queue";
// private static final String COMPANY_QUEUE = "company_queue";
//
// private static String askDB(String request) {
// return request;
// }
//
// public static void main(String[] args) {
// ConnectionFactory factory = new ConnectionFactory();
// factory.setHost("localhost");
//
// Connection clientConnect = null;
// Connection companyConnect = null;
// try {
// clientConnect = factory.newConnection();
// companyConnect = factory.newConnection();
// final Channel companyChannel = companyConnect.createChannel();
// final Channel clientChannel = clientConnect.createChannel();
//
// companyChannel.queueDeclare(COMPANY_QUEUE, true, false, false, null);
// clientChannel.queueDeclare(CLIENT_QUEUE, true, false, false, null);
//
// clientChannel.basicQos(1);
// companyChannel.basicQos(1);
// System.out.println("Initialization complete");
//
// final Consumer companyConsumer = new DefaultConsumer(companyChannel) {
// @Override
// public void handleDelivery(String consumerTag, Envelope envelope,
// AMQP.BasicProperties properties, byte[] body) throws IOException {
// String message = new String(body, "UTF-8");
//
// System.out.println(" [x] Received '" + message + "'");
// }
// };
//
//
//
// Consumer clientConsumer = new DefaultConsumer(clientChannel) {
// @Override
// public void handleDelivery(String consumerTag, Envelope envelope,
// AMQP.BasicProperties properties,
// byte[] body) throws IOException {
// AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
// .correlationId(properties.getCorrelationId()).build();
//
// String response = "";
//
// try {
// String message = new String(body, "UTF-8");
// String[] str = message.split(" ");
// System.out.println("Request: ");
// for (int i = 0; i < str.length; i++) {
// System.out.println(str[i]);
// }
// System.out.println("========");
//
// } catch (RuntimeException e) {
// System.out.println(" [.] " + e.toString());
// } finally {
// clientChannel.basicPublish("", properties.getReplyTo(), replyProps,
// response.getBytes("UTF-8"));
// clientChannel.basicAck(envelope.getDeliveryTag(), false);
// synchronized (this) {
// this.notify();
// }
// }
// }
// };
//
// clientChannel.basicConsume(CLIENT_QUEUE, clientConsumer);
//
// while (true) {
// synchronized(clientConsumer) {
// try {
// clientConsumer.wait();
// }catch (InterruptedException e) {
// e.printStackTrace();
// }
// }
// }
// } catch (IOException | TimeoutException e) {
// e.printStackTrace();
// } finally {
// if (clientConnect != null) {
// try {
// clientConnect.close();
// }catch (IOException _ignore) {}
// }
//
// }
// }
//
// }
