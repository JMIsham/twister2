//
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
package edu.iu.dsc.tws.comms.mpi;

import mpi.Request;

public class MPIRequest {

  public enum RequestType {
    READ,
    WRITE,
  }

  private Request request;

  private RequestType type;

  private int rank;

  public MPIRequest(Request request, RequestType type, int rank) {
    this.request = request;
    this.type = type;
    this.rank = rank;
  }

  public Request getRequest() {
    return request;
  }

  public RequestType getType() {
    return type;
  }
}
