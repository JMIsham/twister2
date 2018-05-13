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
package edu.iu.dsc.tws.rsched.schedulers.k8s;

import java.util.List;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class KubernetesContext extends SchedulerContext {

  public static final int WORKERS_PER_POD_DEFAULT = 1;
  public static final String WORKERS_PER_POD = "kubernetes.workers.per.pod";

  public static final String KUBERNETES_NAMESPACE_DEFAULT = "default";
  public static final String KUBERNETES_NAMESPACE = "kubernetes.namespace";

  public static final boolean NODE_PORT_SERVICE_REQUESTED_DEFAULT = false;
  public static final String NODE_PORT_SERVICE_REQUESTED = "kubernetes.node.port.service.requested";

  public static final int SERVICE_NODE_PORT_DEFAULT = 0;
  public static final String SERVICE_NODE_PORT = "kubernetes.service.node.port";

  public static final int K8S_WORKER_BASE_PORT_DEFAULT = 9999;
  public static final String K8S_WORKER_BASE_PORT = "kubernetes.worker.base.port";

  public static final String WORKER_TRANSPORT_PROTOCOL_DEFAULT = "TCP";
  public static final String WORKER_TRANSPORT_PROTOCOL = "kubernetes.worker.transport.protocol";

  public static final String K8S_IMAGE_PULL_POLICY_NAMESPACE = "IfNotPresent";
  public static final String K8S_IMAGE_PULL_POLICY = "kubernetes.image.pull.policy";

  public static final String K8S_PERSISTENT_STORAGE_CLASS_DEFAULT = "twister2";
  public static final String K8S_PERSISTENT_STORAGE_CLASS = "kubernetes.persistent.storage.class";

  public static final String K8S_STORAGE_ACCESS_MODE_DEFAULT = "ReadWriteMany";
  public static final String K8S_STORAGE_ACCESS_MODE = "kubernetes.storage.access.mode";

  public static final String K8S_STORAGE_RECLAIM_POLICY_DEFAULT = "Retain";
  public static final String K8S_STORAGE_RECLAIM_POLICY = "kubernetes.storage.reclaim.policy";

  // it can be either "system" or "kubernetes". currently not used.
  public static final String PERSISTENT_LOGGING_TYPE_DEFAULT = "system";
  public static final String PERSISTENT_LOGGING_TYPE = "persistent.logging.type";

  public static final boolean K8S_BIND_WORKER_TO_CPU_DEFAULT = false;
  public static final String K8S_BIND_WORKER_TO_CPU = "kubernetes.bind.worker.to.cpu";

  public static final boolean K8S_WORKER_TO_NODE_MAPPING_DEFAULT = false;
  public static final String K8S_WORKER_TO_NODE_MAPPING = "kubernetes.worker.to.node.mapping";

  public static final String K8S_WORKER_MAPPING_KEY = "kubernetes.worker.mapping.key";
  public static final String K8S_WORKER_MAPPING_OPERATOR = "kubernetes.worker.mapping.operator";
  public static final String K8S_WORKER_MAPPING_VALUES = "kubernetes.worker.mapping.values";

  // it can be either "all-same-node", "all-separate-nodes", "none"
  public static final String K8S_WORKER_MAPPING_UNIFORM_DEFAULT = "none";
  public static final String K8S_WORKER_MAPPING_UNIFORM = "kubernetes.worker.mapping.uniform";

  public static int workersPerPod(Config cfg) {
    return cfg.getIntegerValue(WORKERS_PER_POD, WORKERS_PER_POD_DEFAULT);
  }

  public static String namespace(Config cfg) {
    return cfg.getStringValue(KUBERNETES_NAMESPACE, KUBERNETES_NAMESPACE_DEFAULT);
  }

  public static boolean nodePortServiceRequested(Config cfg) {
    return cfg.getBooleanValue(NODE_PORT_SERVICE_REQUESTED, NODE_PORT_SERVICE_REQUESTED_DEFAULT);
  }

  public static int serviceNodePort(Config cfg) {
    return cfg.getIntegerValue(SERVICE_NODE_PORT, SERVICE_NODE_PORT_DEFAULT);
  }

  public static int workerBasePort(Config cfg) {
    return cfg.getIntegerValue(K8S_WORKER_BASE_PORT, K8S_WORKER_BASE_PORT_DEFAULT);
  }

  public static String imagePullPolicy(Config cfg) {
    return cfg.getStringValue(K8S_IMAGE_PULL_POLICY, K8S_IMAGE_PULL_POLICY_NAMESPACE);
  }

  public static String persistentStorageClass(Config cfg) {
    return cfg.getStringValue(K8S_PERSISTENT_STORAGE_CLASS, K8S_PERSISTENT_STORAGE_CLASS_DEFAULT);
  }

  public static String storageAccessMode(Config cfg) {
    return cfg.getStringValue(K8S_STORAGE_ACCESS_MODE, K8S_STORAGE_ACCESS_MODE_DEFAULT);
  }

  public static String storageReclaimPolicy(Config cfg) {
    return cfg.getStringValue(K8S_STORAGE_RECLAIM_POLICY, K8S_STORAGE_RECLAIM_POLICY_DEFAULT);
  }

  public static String persistentLoggingType(Config cfg) {
    return cfg.getStringValue(PERSISTENT_LOGGING_TYPE, PERSISTENT_LOGGING_TYPE_DEFAULT);
  }

  public static String workerTransportProtocol(Config cfg) {
    return cfg.getStringValue(WORKER_TRANSPORT_PROTOCOL, WORKER_TRANSPORT_PROTOCOL_DEFAULT);
  }

  public static boolean bindWorkerToCPU(Config cfg) {
    return cfg.getBooleanValue(K8S_BIND_WORKER_TO_CPU, K8S_BIND_WORKER_TO_CPU_DEFAULT);
  }

  public static boolean workerToNodeMapping(Config cfg) {
    return cfg.getBooleanValue(K8S_WORKER_TO_NODE_MAPPING, K8S_WORKER_TO_NODE_MAPPING_DEFAULT);
  }

  public static String workerMappingKey(Config cfg) {
    return cfg.getStringValue(K8S_WORKER_MAPPING_KEY);
  }

  public static String workerMappingOperator(Config cfg) {
    return cfg.getStringValue(K8S_WORKER_MAPPING_OPERATOR);
  }

  public static List<String> workerMappingValues(Config cfg) {
    return cfg.getStringList(K8S_WORKER_MAPPING_VALUES);
  }

  public static String workerMappingUniform(Config cfg) {
    return cfg.getStringValue(K8S_WORKER_MAPPING_UNIFORM, K8S_WORKER_MAPPING_UNIFORM_DEFAULT);
  }

}
