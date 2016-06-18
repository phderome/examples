/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

/**
 * Demonstrates how to perform simple, state-less transformations via map functions.
 * See also the Scala variant MapFunctionScalaExample.
 *
 * Use cases include e.g. basic data sanitization, data anonymization by obfuscating sensitive data
 * fields (such as personally identifiable information aka PII).
 *
 * ./bin/kafka-console-consumer --zookeeper localhost --topic UppercasedTextLinesTopic
 * Optionally add --property print.key=true to see null keys explicitly.
 *
 * ./bin/kafka-console-consumer --zookeeper localhost --topic UppercasedWithMapTextLinesTopic
 * Optionally add --property print.key=true to see null keys explicitly.
 *
 * Use ./bin/kafka-console-consumer --zookeeper localhost --topic OriginalAndUppercasedTopic --property print.key=true
 * to show original input preserved as key for OriginalAndUppercasedTopic.
 *
 *
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class MapFunctionLambdaExample {

  public static void main(String[] args) throws Exception {
    Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-lambda-example");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // Where to find the corresponding ZooKeeper ensemble.
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // Set up serializers and deserializers, which we will use for overriding the default serdes
    // specified above.
    final Serde<String> stringSerde = Serdes.String();
    final Serde<byte[]> byteArraySerde = Serdes.ByteArray();

    // In the subsequent lines we define the processing topology of the Streams application.
    KStreamBuilder builder = new KStreamBuilder();

    // Read the input Kafka topic into a KStream instance.
    KStream<byte[], String> textLines = builder.stream(byteArraySerde, stringSerde, "TextLinesTopic");

    // Variant 1: using `mapValues`
    KStream<byte[], String> uppercasedWithMapValues = textLines.mapValues(String::toUpperCase);

    // Write (i.e. persist) the results to a new Kafka topic called "UppercasedTextLinesTopic".
    //
    // In this case we can rely on the default serializers for keys and values because their data
    // types did not change, i.e. we only need to provide the name of the output topic.
    uppercasedWithMapValues.to("UppercasedTextLinesTopic");

    // Variant 2: using `map`, modify value only (equivalent to variant 1)
    KStream<byte[], String> uppercasedWithMap = textLines.map((key, value) -> new KeyValue<>(key, value.toUpperCase()));
    uppercasedWithMap.to( "UppercasedWithMapTextLinesTopic");

    // Variant 3: using `map`, modify both key and value
    //
    // Note: Whether, in general, you should follow this artificial example and store the original
    //       value in the key field is debatable and depends on your use case.  If in doubt, don't
    //       do it.
    KStream<String, String> originalAndUppercased = textLines.map((key, value) -> KeyValue.pair(value, value.toUpperCase()));

    // Write the results to a new Kafka topic "OriginalAndUppercasedTopic".
    //
    // In this case we must explicitly set the correct serializers because the default serializers
    // (cf. streaming configuration) do not match the type of this particular KStream instance.
    originalAndUppercased.to(stringSerde, stringSerde, "OriginalAndUppercasedTopic");

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();
  }

}