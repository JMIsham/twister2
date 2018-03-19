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


import java.util.logging.Logger;


import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;


public class KafkaContainer implements IContainer {
  private static final Logger LOG = Logger.getLogger(KafkaContainer.class.getName());

  private Config config;
  private ResourcePlan resourcePlan;
  private int id;

  @Override
  public void init(Config configr, int idr, ResourcePlan resourcePlanr) {
    this.config = configr;
    this.resourcePlan = resourcePlanr;
    this.id = idr;

    if (id == 0) {
      LOG.info("Producing Messages");
      KafkaProducerClient producer = new KafkaProducerClient(null, "sample_topic1", id);
      producer.process();
    } else {
      KafkaConsumerClient consumer = new KafkaConsumerClient(null, "sample_topic1", id);
      consumer.process();
    }

  }


}
