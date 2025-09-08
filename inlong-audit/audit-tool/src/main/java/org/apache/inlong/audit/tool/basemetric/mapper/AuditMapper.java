package org.apache.inlong.audit.tool.basemetric.mapper;


import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.inlong.audit.tool.basemetric.vo.AuditDataVo;

import java.util.List;

@Mapper
public interface AuditMapper {
    @Select("SELECT * FROM audit_data WHERE log_ts > NOW() - INTERVAL #{seconds} SECOND")
    List<AuditDataVo> queryAuditDataBySeconds(@Param("seconds") Integer seconds);
}
