package org.apache.inlong.agent.metrics;

import org.apache.inlong.common.metric.MetricDomain;
import org.apache.inlong.common.metric.MetricItemSet;

@MetricDomain(name = "Agent")
public class AgentMetricItemSet extends MetricItemSet<AgentMetricItem> {

    /**
     * Constructor
     *
     * @param name
     */
    public AgentMetricItemSet(String name) {
        super(name);
    }

    @Override
    protected AgentMetricItem createItem() {
        return new AgentMetricItem();
    }
}
