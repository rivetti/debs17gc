package tech.debs17gc.system.input.rmq;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHbyte[] WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.hobbit.core.data.RabbitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;

/**
 * RabbitMQ source (consumer) which reads from a queue and acknowledges messages on checkpoints. When checkpointing is enabled, it
 * guarantees exactly-once processing semantics.
 *
 * RabbitMQ requires messages to be acknowledged. On failures, RabbitMQ will re-resend all messages which have not been acknowledged
 * previously. When a failure occurs directly after a completed checkpoint, all messages part of this checkpoint might be processed again
 * because they couldn't be acknowledged before failure. This case is handled by the {@link MessageAcknowledgingSourceBase} base class which
 * deduplicates the messages using the correlation id.
 *
 * RabbitMQ's Delivery Tags do NOT represent unique ids / offsets. That's why the source uses the Correlation ID in the message properties
 * to check for duplicate messages. Note that the correlation id has to be set at the producer. If the correlation id is not set, messages
 * may be produced more than once in corner cases.
 *
 * This source can be operated in three different modes:
 *
 * 1) Exactly-once (when checkpointed) with RabbitMQ transactions and messages with unique correlation IDs. 2) At-least-once (when
 * checkpointed) with RabbitMQ transactions but no deduplication mechanism (correlation id is not set). 3) No strong delivery guarantees
 * (without checkpointing) with RabbitMQ auto-commit mode.
 *
 * Users may overwrite the setupConnectionFactory() method to pass their setup their own ConnectionFactory in case the constructor
 * parameters are not sufficient.
 *
 * @param <byte[]>
 *            The type of the data read from RabbitMQ.
 */
public class MyRMQSourceNotBlocking extends RichSourceFunction<byte[]> implements ResultTypeQueryable<byte[]>, ParallelSourceFunction<byte[]> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(MyRMQSourceNotBlocking.class);

	private final String host;
	private final String queueName;

	private transient RabbitQueue queue;

	protected transient QueueingConsumer consumer;

	private transient volatile String tag = null;
	private transient volatile boolean running;

	/**
	 * Creates a new RabbitMQ source. For exactly-once, you must set the correlation ids of messages at the producer. The correlation id
	 * must be unique. Otherwise the behavior of the source is undefined. In doubt, set {@param usesCorrelationId} to false. When
	 * correlation ids are not used, this source has at-least-once processing semantics when checkpointing is enabled.
	 * 
	 * @param rmqConnectionConfig
	 *            The RabbiMQ connection configuration {@link RMQConnectionConfig}.
	 * @param queueName
	 *            The queue to receive messages from.
	 * @param usesCorrelationId
	 *            Whether the messages received are supplied with a <b>unique</b> id to deduplicate messages (in case of failed
	 *            acknowledgments). Only used when checkpointing is enabled.
	 * @param deserializationSchema
	 *            A {@link DeserializationSchema} for turning the bytes received into Java objects.
	 */
	public MyRMQSourceNotBlocking(String host, String queueName) {
		this.host = host;
		this.queueName = queueName;
	}

	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
		LOG.info("thread: {} ({})", Thread.currentThread().getName(), Thread.currentThread().getId());
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(this.host);
		try {
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();
			if (channel == null) {
				throw new RuntimeException("None of RabbitMQ channels are available");
			}
			channel.basicQos(1); // Hobbit?
			channel.queueDeclare(queueName, false, false, true, null);// Hobbit?
			queue = new RabbitQueue(channel, queueName);

			consumer = new QueueingConsumer(channel);

			LOG.debug("Starting RabbitMQ source");
			channel.basicConsume(queue.getName(), false, consumer);

		} catch (IOException e) {
			throw new RuntimeException("Cannot create RMQ connection with " + queueName + " at " + host, e);
		}
		this.running = true;
	}

	@Override
	public void close() throws Exception {
		IOException t = null;
		Channel channel = queue.getChannel();
		Connection connection = channel.getConnection();
		try {
			channel.close();
		} catch (IOException e) {
			t = e;
		}

		try {
			connection.close();
		} catch (IOException e) {
			if (t != null) {
				LOG.warn("Both channel and connection closing failed. Logging channel exception and failing with connection exception", t);
			}
			t = e;
		}
		if (t != null) {
			throw new RuntimeException("Error while closing RMQ connection with " + queueName + " at " + host, t);
		}
	}

	@Override
	public void run(SourceContext<byte[]> ctx) throws Exception {
		Channel channel = queue.getChannel();
		this.tag = channel.basicConsume(queue.getName(), false, new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
					throws IOException {
				getChannel().basicAck(envelope.getDeliveryTag(), false);
				ctx.collect(body);
			}
		});

		while (running) {
			Thread.yield();
		}

	}

	@Override
	public void cancel() {
		this.running = false;
		try {
			queue.getChannel().basicCancel(tag);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public TypeInformation<byte[]> getProducedType() {
		return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
	}
}
