//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.examples.basic.kafka;


import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class KafkaConsumerClient {
  private static final Logger LOG = Logger.getLogger(KafkaConsumerClient.class.getName());
  private  Properties props;
  private String topicName;
  private int id;

  public KafkaConsumerClient(Properties props, String topicName, int id) {
    this.topicName = topicName;
    this.props = props;
    this.id = id;

    if (props == null) {
      this.props = new Properties();
      this.props.put("bootstrap.servers", "localhost:9092");
      this.props.put("group.id", "test");
      this.props.put("enable.auto.commit", "true");
      this.props.put("auto.commit.interval.ms", "1000");
      this.props.put("session.timeout.ms", "30000");
      this.props.put("key.deserializer",
          "org.apache.kafka.common.serialization.StringDeserializer");
      this.props.put("value.deserializer",
          "org.apache.kafka.common.serialization.StringDeserializer");
    }
    LOG.info(String.format("kafka consumer created from process: %d", id));
  }

  public void process() {

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

    consumer.subscribe(Arrays.asList(topicName));
    System.out.println("Subscribed to topic " + topicName);

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        LOG.info(String.format("value = %s, received process = %d", record.value(), id));
      }
    }
  }

}

