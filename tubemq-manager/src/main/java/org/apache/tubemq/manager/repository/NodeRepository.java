package org.apache.tubemq.manager.repository;

import org.apache.tubemq.manager.entry.NodeEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface NodeRepository extends JpaRepository<NodeEntry, Long> {

    NodeEntry findNodeEntryByClusterIdIsAndMasterIsTrue(int clusterId);
}
