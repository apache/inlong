package org.apache.tubemq.manager.repository;

import org.apache.tubemq.manager.entry.BusinessEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BusinessRepository extends JpaRepository<BusinessEntry, Long> {
   public BusinessEntry findByName(String name);
}
