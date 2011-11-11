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

        TestNode master = new TestNode("master", false, 1.0f, 0);
        TestNode node2 = new TestNode("node2", true, 1.0f, 0);
        TestNode node3 = new TestNode("node3", true, 1.0f, 0);
        List<TestNode> nodes = Arrays.asList(master, node2, node3);
        
        ReplicaSetSecondaryStrategy strat = new DefaultReplicaSetSecondaryStrategy(2);
        
        assertNotNull(strat.select(null, null, nodes));
    }
    
    @Test
    public void testEvenDistribution() {

        TestNode master = new TestNode("master", false, 1.0f, 0);
        TestNode node2 = new TestNode("node2", true, 1.0f, 0);
        TestNode node3 = new TestNode("node3", true, 1.0f, 0);
        List<TestNode> nodes = Arrays.asList(master, node2, node3);
        
        ReplicaSetSecondaryStrategy strat = new DefaultReplicaSetSecondaryStrategy(2);
        Map<TestNode, Float> expected = new HashMap<TestNode, Float>();
        expected.put(master, 0.0f);
        expected.put(node2, .5f);
        expected.put(node3, .5f);
        assertSelectionDistribution(expected, strat, nodes);
    }
    
    @Test
    public void testDefaultEvenDistributionWithSlowNode() {

        ReplicaSetSecondaryStrategy strat = new DefaultReplicaSetSecondaryStrategy(2);
        assertEvenDistributionWithSlowNode(strat);
    }
    
    @Test
    public void testQueueingNodesAreBad() {

        ReplicaSetSecondaryStrategy strat = new NoQueueStrategy(2, 0);
        TestNode master = new TestNode("master", false, 1.0f, 0);
        TestNode node2 = new TestNode("node2", true, 1.0f, 0);
        TestNode node3 = new TestNode("node3", true, 1.0f, 0);
        TestNode node4 = new TestNode("node4", true, 1.0f, 100);
        List<TestNode> nodes = Arrays.asList(master, node2, node3, node4);
        
        Map<TestNode, Float> expected = new HashMap<TestNode, Float>();
        expected.put(master, 0.0f);
        expected.put(node2, .5f);
        expected.put(node3, .5f);
        expected.put(node4, 0.0f);
        assertSelectionDistribution(expected, strat, nodes);
    }

    private void assertEvenDistributionWithSlowNode(ReplicaSetSecondaryStrategy strat) {
        TestNode master = new TestNode("master", false, 1.0f, 0);
        TestNode node2 = new TestNode("node2", true, 1.0f, 0);
        TestNode node3 = new TestNode("node3", true, 1.0f, 0);
        TestNode node4 = new TestNode("node4", true, 1.0f, 0);
        TestNode node5 = new TestNode("node5", true, 10.0f, 0);
        List<TestNode> nodes = Arrays.asList(master, node2, node3, node4);
        
        Map<TestNode, Float> expected = new HashMap<TestNode, Float>();
        expected.put(master, 0.0f);
        expected.put(node2, .33f);
        expected.put(node3, .33f);
        expected.put(node4, 0.33f);
        expected.put(node5, 0.0f);
        assertSelectionDistribution(expected, strat, nodes);
    }
    
    public void assertSelectionDistribution(Map<TestNode, Float> expected, ReplicaSetSecondaryStrategy strat, List<TestNode> nodes) {
        int iterations = 10000;
        int fudgePercent = 2;
        Map<TestNode, Integer> histogram = new HashMap<TestNode, Integer>();
        for (int i = 0; i < iterations; i++) {
            TestNode winner = strat.select(null, null, nodes);
            Integer currentCount = histogram.get(winner);
            Integer newCount = (currentCount == null ? 0 : currentCount) + 1;
            histogram.put(winner, newCount);
        }
        System.out.println(histogram);
        for (Map.Entry<TestNode, Float> entry : expected.entrySet()) {
            Integer count = histogram.get(entry.getKey());
            int expectedPercent = Math.round((entry.getValue() * iterations)/100);
            int actualPercent = count == null ? 0 : (int)Math.round(((100.0 * count) / iterations));
            assertTrue(Math.abs(expectedPercent - actualPercent) < fudgePercent, String.format("%s seen %d%% but expected %d%%", entry.getKey(), actualPercent, expectedPercent));
        }
    }
    
    static class TestNode implements ReplicaSetNode {
        private final String _name;
        private final boolean _secondary;
        private final float _pingTime;
        private final int _queueSize;

        public TestNode(String name, boolean secondary, float pingTime, int queueSize) {
            this._name = name;
            this._secondary = secondary;
            this._pingTime = pingTime;
            this._queueSize = queueSize;
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
        
        public String toString() {
            return "Node("+_name +","+_secondary+","+_pingTime+","+_queueSize+")";
        }

        @Override
        public int getQueueSize() {
            return _queueSize;
        }
        
    }
}

