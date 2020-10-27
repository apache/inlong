package org.apache.tubemq.manager.entry;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Data;

/**
 * node machine for tube cluster. broker/master/standby
 */
@Entity
@Table(name = "node")
@Data
public class NodeEntry {
    @Id
    @GeneratedValue(strategy= GenerationType.AUTO)
    private long brokerId;

    private boolean master;

    private boolean standby;

    private boolean broker;

    private String ip;

    private int port;

    private int webPort;

    private int clusterId;
}
