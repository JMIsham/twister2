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
package edu.iu.dsc.tws.task.api;

public final class Operations {
  private Operations() {
  }

  public static final String PARTITION = "partition";
  public static final String REDUCE = "reduce";
  public static final String ALL_REDUCE = "all_reduce";
  public static final String ALL_GATHER = "all_gather";
  public static final String KEYED_GATHER = "keyed_gather";
  public static final String KEYED_REDUCE = "keyed_reduce";
}
