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
package edu.iu.dsc.tws.executor.comm;

import java.util.Set;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.EdgeGenerator;
import edu.iu.dsc.tws.task.api.Operations;
import edu.iu.dsc.tws.task.graph.Edge;

public class ParallelOperationFactory {
  private TWSChannel channel;

  private Config config;

  private TaskPlan taskPlan;

  private EdgeGenerator edgeGenerator;

  public ParallelOperationFactory(Config cfg, TWSChannel network, TaskPlan plan, EdgeGenerator e) {
    this.channel = network;
    this.config = cfg;
    this.taskPlan = plan;
    this.edgeGenerator = e;
  }

  public IParallelOperation build(Edge edge, Set<Integer> sources, Set<Integer> dests) {
    if (!edge.isKeyed()) {
      if (Operations.PARTITION.equals(edge.getOperation())) {
        PartitionOperation partitionOp = new PartitionOperation(config, channel, taskPlan);
        partitionOp.prepare(sources, dests, edgeGenerator, edge.getDataType(), edge.getName());
        return partitionOp;
      } else if (Operations.BROADCAST.equals(edge.getOperation())) {
        BroadcastOperation bcastOp = new BroadcastOperation(config, channel, taskPlan);
        // get the first as the source
        bcastOp.prepare(sources.iterator().next(), dests, edgeGenerator, edge.getDataType(),
            edge.getName());
        return bcastOp;
      }
    } else {
      if (Operations.PARTITION.equals(edge.getOperation())) {
        PartitionOperation partitionOp = new PartitionOperation(config, channel, taskPlan);
        partitionOp.prepare(sources, dests, edgeGenerator, edge.getDataType(),
            edge.getKeyType(), edge.getName());
        return partitionOp;
      }
    }
    return null;
  }

  public IParallelOperation build(Edge edge, Set<Integer> sources, Set<Integer> dests,
                                 DataType dataType, DataType keyType) {
    if (Operations.PARTITION.equals(edge.getOperation())) {
      PartitionOperation partitionOp = new PartitionOperation(config, channel, taskPlan);
      partitionOp.prepare(sources, dests, edgeGenerator, dataType, keyType, edge.getName());
      return partitionOp;
    }
    return null;
  }
}
