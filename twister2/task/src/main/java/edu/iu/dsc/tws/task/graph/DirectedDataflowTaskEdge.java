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
package edu.iu.dsc.tws.task.graph;

import java.util.Comparator;

public class DirectedDataflowTaskEdge<TV, TE> {
  protected TV sourceTaskVertex;
  protected TV targetTaskVertex;
  protected TE taskEdge;

  protected Comparator<TV> vertexComparator;

  public DirectedDataflowTaskEdge() {
  }

  public DirectedDataflowTaskEdge(Comparator<TV> vertexComparator) {
    this.vertexComparator = vertexComparator;
  }

  public DirectedDataflowTaskEdge(TV sourceTaskVertex, TV targetTaskVertex,
                                  TE taskEdge, Comparator<TV> vertexComparator) {
    this.sourceTaskVertex = sourceTaskVertex;
    this.targetTaskVertex = targetTaskVertex;
    this.taskEdge = taskEdge;
    this.vertexComparator = vertexComparator;
  }

  public TE getTaskEdge() {
    return taskEdge;
  }

  public void setTaskEdge(TE taskEdge) {
    this.taskEdge = taskEdge;
  }

  public TV getSourceTaskVertex() {
    return sourceTaskVertex;
  }

  public void setSourceTaskVertex(TV sourceTaskVertex) {
    this.sourceTaskVertex = sourceTaskVertex;
  }

  public TV getTargetTaskVertex() {
    return targetTaskVertex;
  }

  public void setTargetTaskVertex(TV targetTaskVertex) {
    this.targetTaskVertex = targetTaskVertex;
  }


}

