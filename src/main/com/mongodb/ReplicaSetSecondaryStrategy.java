package com.mongodb;

import java.util.List;


public interface ReplicaSetSecondaryStrategy {
    public <T extends ReplicaSetNode> T select(String pTagKey, String pTagValue, List<T> pNodes);
}
