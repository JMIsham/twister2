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
package edu.iu.dsc.tws.rsched.core;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;

public class SchedulerContext extends Context {
  public static final String STATE_MANAGER_CLASS = "twister2.class.state.manager";
  public static final String SCHEDULER_CLASS = "twister2.class.scheduler";
  public static final String LAUNCHER_CLASS = "twister2.class.launcher";
  public static final String UPLOADER_CLASS = "twister2.class.uploader";

  // this is a configuration key to hold the original jar file location of the job
  public static final String PACKAGE_FILE = "twister2.job.package.file";
  public static final String JOB_NAME = "twister2.job.name";
  public static final String STATE_MANAGER_ROOT_PATH = "twister2.state.manager.root.path";

  /**
   * Internal configuration
   */
  public static final String JOB_PACKAGE_URI = "twister2.job.package.uri";

  public static String stateManagerClass(Config cfg) {
    return cfg.getStringValue(STATE_MANAGER_CLASS);
  }

  public static String schedulerClass(Config cfg) {
    return cfg.getStringValue(SCHEDULER_CLASS);
  }

  public static String uploaderClass(Config cfg) {
    return cfg.getStringValue(UPLOADER_CLASS);
  }

  public static String launcherClass(Config cfg) {
    return cfg.getStringValue(LAUNCHER_CLASS);
  }

  public static String getJobPackageFile(Config cfg) {
    return cfg.getStringValue(PACKAGE_FILE);
  }

  public static String getJobName(Config cfg) {
    return cfg.getStringValue(JOB_NAME);
  }

  public static String getStateManegerRootPath(Config cfg) {
    return cfg.getStringValue(STATE_MANAGER_ROOT_PATH);
  }
}