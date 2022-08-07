package org.apache.inlong.sdk.dataproxy.utils;

import org.apache.inlong.sdk.dataproxy.config.HostInfo;
import org.apache.inlong.sdk.dataproxy.network.NettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HashRing {
    private int virtualNode = 1000;
    private TreeMap<String, NettyClient> virtualNode2RealNode = new TreeMap<>();
    private Map<HostInfo, NettyClient> nodeList = new HashMap<>();
    private static final HashRing instance = new HashRing();
    private static final Logger LOGGER = LoggerFactory.getLogger(HashRing.class);

    public static HashRing getInstance() {
        return instance;
    }

    public String node2VirtualNode(HostInfo node, int index) {
        return  "virtual&&" + index + "&&" + node.toString();
    }

    public void initHashRing(Map<HostInfo, NettyClient> ipList) {
        this.virtualNode2RealNode = new TreeMap<>();
        this.nodeList = ipList;
        Set<HostInfo> hosts = this.nodeList.keySet();
        for (HostInfo host : hosts) {
            NettyClient node = this.nodeList.get(host);
            for (int i = 0; i < this.virtualNode; i++) {
                String key = node2VirtualNode(host, i);
                String hash = ConsistencyHashUtil.hashMurMurHash(key);
                virtualNode2RealNode.put(hash, node);
            }
        }
        LOGGER.info("init hash ring {}", this.virtualNode2RealNode);
    }

    private void setVirtualNode(int virtualNode) {
        this.virtualNode = virtualNode;
    }

    public NettyClient getNode(String key) {
        String hash = ConsistencyHashUtil.hashMurMurHash(key);
        SortedMap<String, NettyClient> tailMap = this.virtualNode2RealNode.tailMap(hash);
        NettyClient node;
        if (tailMap.isEmpty()) {
            node = this.virtualNode2RealNode.get(this.virtualNode2RealNode.firstKey());
        } else {
            node = this.virtualNode2RealNode.get(tailMap.firstKey());
        }
        LOGGER.info("{} located to {}", key, node);
        return node;
    }

    public void appendNode(HostInfo host, NettyClient node) {
        this.nodeList.put(host, node);
        for (int i = 0; i < this.virtualNode; i++) {
            String key = node2VirtualNode(host, i);
            String hash = ConsistencyHashUtil.hashMurMurHash(key);
            virtualNode2RealNode.put(hash, node);
            LOGGER.info("append node {}", host);
        }
    }

    public void extendNode(Map<HostInfo, NettyClient> nodes) {
        this.nodeList.putAll(nodes);
        Set<HostInfo> hosts = nodes.keySet();
        for (HostInfo host : hosts) {
            for (int i = 0; i < this.virtualNode; i++) {
                String key = node2VirtualNode(host, i);
                String hash = ConsistencyHashUtil.hashMurMurHash(key);
                virtualNode2RealNode.put(hash, nodes.get(host));
            }
        }
        LOGGER.info("append node list {}", nodes.keySet());
    }

    public void deleteNode(HostInfo host) {
        this.nodeList.remove(host);
        for (int i = 0; i < this.virtualNode; i++) {
            String hash = ConsistencyHashUtil.hashMurMurHash(node2VirtualNode(host, i));
            virtualNode2RealNode.remove(hash);
        }
        LOGGER.info("remove node {}", host);
    }

    public void removeNode(List<HostInfo> hosts) {
        for (HostInfo host : hosts) {
            this.nodeList.remove(host);
            for (int i = 0; i < this.virtualNode; i++) {
                String hash = ConsistencyHashUtil.hashMurMurHash(node2VirtualNode(host, i));
                virtualNode2RealNode.remove(hash);
            }
        }
        LOGGER.info("remove node list {}", hosts);
    }
}
