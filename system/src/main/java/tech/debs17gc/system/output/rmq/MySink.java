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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY Kbyte[]D, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.debs17gc.system.output.rmq;

import java.io.IOException;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.hobbit.core.data.RabbitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import tech.debs17gc.common.Common;
import tech.debs17gc.common.configuration.Config;
import tech.debs17gc.common.metadata.MetaData;
import tech.debs17gc.system.input.oo.BOOParser;
import tech.debs17gc.system.output.bytearray.ByteArraySerializerUtils;

/**
 * A Sink for publishing data into RabbitMQ
 * 
 * @param <byte[]>
 */
public class MySink extends RichSinkFunction<Tuple5<Integer, Integer, Integer, Integer, Double>> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(MySink.class);

	private final String queueName;
	private final String host;
	private transient RabbitQueue queue;

	private int anomaliesCount = 0;
	private int transitionNum = 0;

	/*
	 * PERF
	 */
	private double count = 0;
	private double step = 10000;
	private long delay = 0;
	private long totalDelay = 0;

	/**
	 * @param rmqConnectionConfig
	 *            The RabbitMQ connection configuration {@link RMQConnectionConfig}.
	 * @param queueName
	 *            The queue to publish messages to.
	 * @param schema
	 *            A {@link SerializationSchema} for turning the Java objects received into bytes
	 */
	public MySink(String host, String queueName) {
		this.host = host;
		this.queueName = queueName;
	}

	@Override
	public void open(Configuration config) throws Exception {
		LOGGER.info("thread: {} ({})", Thread.currentThread().getName(), Thread.currentThread().getId());
		Config.load();
		MetaData.load();

		this.count = 0;
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(this.host);
		try {
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();
			if (channel == null) {
				throw new RuntimeException("None of RabbitMQ channels are available");
			}
			channel.basicQos(1);
			channel.queueDeclare(queueName, false, false, true, null);
			queue = new RabbitQueue(channel, queueName);
		} catch (IOException e) {
			throw new RuntimeException("Error while creating the channel", e);
		}

		this.transitionNum = Config.getTransitionsNum();
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to RMQ.
	 *
	 * @param value
	 *            The incoming data
	 */
	@Override
	public void invoke(Tuple5<Integer, Integer, Integer, Integer, Double> value) {
		// TODO RMV for production

		if (count == 0) {
			LOGGER.info("first tuple at {}", System.nanoTime());
		} else if (count > 0 && count % step == 0) {
			totalDelay += delay / step;
			LOGGER.info("count: {}, delay: {}, total: {}, avg: {}, totalAvg: {}", count, delay, totalDelay, delay / step,
					totalDelay * step / count);
			delay = 0;
		}
		this.count++;
		long start = System.nanoTime();


		try {
			if (value.f2 == -1) {
				LOGGER.info("last tuple at {}", System.nanoTime());
				LOGGER.info("count: {}", count);
				LOGGER.info("Sending Termination message {}", Common.TERMINATION_MESSAGE);
				Channel channel = queue.getChannel();
				channel.basicPublish("", queue.getName(), MessageProperties.PERSISTENT_BASIC, Common.TERMINATION_MESSAGE_BYTES);
			} else {

				Channel channel = queue.getChannel();
				channel.basicPublish("", queue.getName(), MessageProperties.PERSISTENT_BASIC,
						ByteArraySerializerUtils.serialize(anomaliesCount, value, transitionNum));
				anomaliesCount++;

			}
		} catch (IOException e) {
			throw new RuntimeException("Cannot send RMQ message " + queueName + " at " + host, e);

		}
		delay += System.nanoTime() - start;

	}

	@Override
	public void close() {
		Exception t = null;
		Channel channel = queue.getChannel();
		Connection connection = channel.getConnection();
		try {
			channel.close();
		} catch (Exception e) {
			t = e;
		}

		try {
			connection.close();
		} catch (IOException e) {
			if (t != null) {
				LOGGER.warn("Both channel and connection closing failed. Logging channel exception and failing with connection exception",
						t);
			}
			t = e;
		}
		if (t != null) {
			throw new RuntimeException("Error while closing RMQ connection with " + queueName + " at " + host, t);
		}
	}

}
