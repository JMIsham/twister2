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

import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;



public class KafkaProducerClient {
  private static final Logger LOG = Logger.getLogger(KafkaProducerClient.class.getName());
  private Properties props;
  private String topicName;
  private int id;

  public KafkaProducerClient(Properties props, String topicName, int id) {
    this.topicName = topicName;
    this.id = id;
    if (props == null) {
      this.props = new Properties();
      this.props.put("bootstrap.servers", "localhost:9092");
      this.props.put("acks", "all");
      this.props.put("retries", 0);
      this.props.put("batch.size", 16384);
      this.props.put("linger.ms", 1);
      this.props.put("buffer.memory", 33554432);
      this.props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      this.props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }
    LOG.info(String.format("kafka producer created from process: %d", this.id));

  }
  public void process() {

    Producer<String, String> producer = new KafkaProducer<>(props);

    for (int i = 0; i < 2000; i++) {
      producer.send(new ProducerRecord<String, String>(topicName,
          Integer.toString(i), Integer.toString(i)));
      LOG.info(String.format("producing message : %d from %d", i, id));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
    producer.close();
  }

}

