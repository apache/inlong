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

    @Select("SELECT * FROM audit_data WHERE (audit_id=1 OR audit_id=2) AND log_ts > NOW() - INTERVAL #{seconds} SECOND")
    public List<AuditDataVo> queryAllApiAuditDataBySecond(@Param("seconds") Integer seconds);

    @Select("SELECT * FROM audit_data WHERE (audit_id=3 OR audit_id=4) AND log_ts > NOW() - INTERVAL #{seconds} SECOND")
    public List<AuditDataVo> queryAllAgentAuditDataBySecond(@Param("seconds") Integer seconds);

    @Select("SELECT * FROM audit_data WHERE (audit_id=5 OR audit_id=6) AND log_ts > NOW() - INTERVAL #{seconds} SECOND")
    public List<AuditDataVo> queryAllDataproxyAuditDataBySecond(@Param("seconds") Integer seconds);

    @Select("SELECT count, size, delay FROM audit_data WHERE audit_id=1 and log_ts > NOW() - INTERVAL #{seconds} SECOND")
    public List<AuditDataVo> queryInlongApiReceiveSuccessBySecond(@Param("seconds") Integer seconds);

    @Select("SELECT count, size, delay FROM audit_data WHERE audit_id=2 and log_ts > NOW() - INTERVAL #{seconds} SECOND")
    public List<AuditDataVo> queryInlongApiSendSuccessBySecond(@Param("seconds") Integer seconds);

    @Select("SELECT count, size, delay FROM audit_data WHERE audit_id=3 and log_ts > NOW() - INTERVAL #{seconds} SECOND")
    public List<AuditDataVo> queryInlongAgentReceiveSuccessBySecond(@Param("seconds") Integer seconds);

    @Select("SELECT count, size, delay FROM audit_data WHERE audit_id=4 and log_ts > NOW() - INTERVAL #{seconds} SECOND")
    public List<AuditDataVo> queryInlongAgentSendSuccessBySecond(@Param("seconds") Integer seconds);

    @Select("SELECT count, size, delay FROM audit_data WHERE audit_id=5 and log_ts > NOW() - INTERVAL #{seconds} SECOND")
    public List<AuditDataVo> queryInlongDataProxyReceiveSuccessBySecond(@Param("seconds") Integer seconds);

    @Select("SELECT count, size, delay FROM audit_data WHERE audit_id=6 and log_ts > NOW() - INTERVAL #{seconds} SECOND")
    public List<AuditDataVo> queryInlongDataProxySendSuccessBySecond(@Param("seconds") Integer seconds);


    @Select("SELECT count, size, delay FROM audit_data WHERE  audit_id = 1 and log_ts > NOW() - INTERVAL #{seconds} SECOND ")
    public List<AuditDataVo> queryInlongApiReceiveSuccessBySecondAndId(@Param("seconds") Integer seconds,@Param("streamId") String streamId,@Param("groupId") String groupId);

    @Select("SELECT count, size, delay FROM audit_data WHERE audit_id = 1 and log_ts > NOW() - INTERVAL #{seconds} SECOND AND audit_id = 2")
    public List<AuditDataVo> queryInlongApiSendSuccessBySecondAndId(@Param("seconds") Integer seconds,@Param("streamId") String streamId,@Param("groupId") String groupId);
}
