/**
 * Copyright (C) 2011 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import org.testng.annotations.Test;


import com.mongodb.util.TestCase;

import java.util.*;


public class ReplicaSetStatusTest extends TestCase {

    @Test
    public void testFindASecondary() {

        TestNode node1 = new TestNode(false, 1.0f);
        TestNode node2 = new TestNode(true, 1.0f);
        TestNode node3 = new TestNode(true, 1.0f);
        List<TestNode> nodes = Arrays.asList(node1, node2, node3);
        
        ReplicaSetSecondaryStrategy strat = new ReplicaSetStatus.DefaultReplicaSetSecondaryStrategy(2);
        
        assertNotNull(strat.select(null, null, nodes));
    }
    
    static class TestNode implements ReplicaSetNode {
        private final boolean _secondary;
        private final float _pingTime;
        
        public TestNode(boolean secondary, float pingTime) {
            this._secondary = secondary;
            this._pingTime = pingTime;
        }

        @Override
        public boolean secondary() {
            return _secondary;
        }

        @Override
        public boolean checkTag(String key, String value) {
            return false;
        }

        @Override
        public float getPingTime() {
            return _pingTime;
        }
        
    }
}

