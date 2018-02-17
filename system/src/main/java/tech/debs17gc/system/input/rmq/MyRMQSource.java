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
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.hobbit.core.data.RabbitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import tech.debs17gc.common.configuration.Config;
import tech.debs17gc.common.metadata.MetaData;

public class MyRMQSource extends RichSourceFunction<byte[]> implements ResultTypeQueryable<byte[]>, ParallelSourceFunction<byte[]> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(MyRMQSource.class);

	private final String host;
	private final String queueName;

	private transient RabbitQueue queue;

	protected transient QueueingConsumer consumer;

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
	public MyRMQSource(String host, String queueName) {
		this.host = host;
		this.queueName = queueName;
	}

	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
		Config.load();
		MetaData.load();
		
		LOGGER.info("thread: {} ({})", Thread.currentThread().getName(), Thread.currentThread().getId());
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(this.host);
		try {
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();
			if (channel == null) {
				throw new RuntimeException("None of RabbitMQ channels are available");
			}
			channel.basicQos(Config.getQoSPrefetch()); // Hobbit?
			channel.queueDeclare(queueName, false, false, true, null);// Hobbit?
			queue = new RabbitQueue(channel, queueName);

			consumer = new QueueingConsumer(channel);

			LOGGER.debug("Starting RabbitMQ source");
			channel.basicConsume(queue.getName(), false, consumer);

		} catch (IOException e) {
			throw new RuntimeException("Cannot create RMQ connection with " + queueName + " at " + host, e);
		}
		running = true;
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
				LOGGER.warn("Both channel and connection closing failed. Logging channel exception and failing with connection exception", t);
			}
			t = e;
		}
		if (t != null) {
			throw new RuntimeException("Error while closing RMQ connection with " + queueName + " at " + host, t);
		}
	}

	@Override
	public void run(SourceContext<byte[]> ctx) throws Exception {
		while (running) {
			// synchronized (ctx.getCheckpointLock()) {

			QueueingConsumer.Delivery delivery = consumer.nextDelivery();
			byte[] result = delivery.getBody();
			ctx.collect(result);

			consumer.getChannel().basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			// }
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

	@Override
	public TypeInformation<byte[]> getProducedType() {
		return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
	}
}
