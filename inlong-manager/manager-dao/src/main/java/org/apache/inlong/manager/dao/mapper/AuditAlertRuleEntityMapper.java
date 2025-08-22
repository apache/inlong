// manager-dao/src/main/java/org/apache/inlong/manager/dao/mapper/AuditAlertRuleEntityMapper.java
package org.apache.inlong.manager.dao.mapper;

import org.apache.inlong.manager.dao.entity.AuditAlertRuleEntity;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AuditAlertRuleEntityMapper {
    int insert(AuditAlertRuleEntity entity);
    int updateById(AuditAlertRuleEntity entity);
    int deleteById(Integer id);
    AuditAlertRuleEntity selectById(Integer id);
    List<AuditAlertRuleEntity> selectByGroupAndStream(String inlongGroupId, String inlongStreamId);
    List<AuditAlertRuleEntity> selectEnabledRules(); // 查询所有启用的策略
}