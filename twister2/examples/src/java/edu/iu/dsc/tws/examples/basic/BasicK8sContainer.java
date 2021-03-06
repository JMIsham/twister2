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
package edu.iu.dsc.tws.examples.basic;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;


public class BasicK8sContainer implements IContainer {
  private static final Logger LOG = Logger.getLogger(BasicK8sContainer.class.getName());

  @Override
  public void init(Config config, int id, ResourcePlan resourcePlan) {
    LOG.info("BasicK8sContainer started. My ID: " + id + " :))))))))))))))))))))) ");
  }



}
