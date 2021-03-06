//  Copyright 2017 Twitter. All rights reserved.
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
package edu.iu.dsc.tws.connectors.kafka;

import java.util.List;
import java.util.regex.Pattern;

import com.sun.istack.internal.Nullable;



public class KafkaTopicDescription {
  private final List<String> fixedTopics;
  private final Pattern topicPattern;

  public KafkaTopicDescription(@Nullable List<String> fixedTopics, @Nullable Pattern topicPattern) {
    if ((fixedTopics != null && topicPattern != null)
            || (fixedTopics == null && topicPattern == null)) {
      throw new IllegalArgumentException("Exactly on of the topic must be provided");
    }

    if (fixedTopics != null && fixedTopics.isEmpty()) {
      throw new IllegalArgumentException("the fixed topic cannot be empty");
    }
    this.fixedTopics = fixedTopics;
    this.topicPattern = topicPattern;
  }

  public boolean isFixedTopics() {
    return fixedTopics != null;
  }

  public boolean isTopicPattern() {
    return topicPattern != null;
  }

  public List<String> getFixedTopics() {
    return fixedTopics;
  }

  public Pattern getTopicPattern() {
    return topicPattern;
  }

}
