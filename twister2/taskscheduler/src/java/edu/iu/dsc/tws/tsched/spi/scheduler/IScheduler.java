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

package edu.iu.dsc.tws.tsched.spi.scheduler;

import edu.iu.dsc.tws.tsched.spi.common.Config;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public interface IScheduler extends AutoCloseable {

  void initialize(Config config, Config runtime);

  void close();

  boolean schedule(TaskSchedulePlan taskscheduleplan);

  //boolean kill(Scheduler.KillJobRequest jobrequest);

  //boolean restart(Scheduler.RestartJobRequest jobrequest);

  //boolean update(Scheduler.UpdateJobRequest jobrequest);

}
