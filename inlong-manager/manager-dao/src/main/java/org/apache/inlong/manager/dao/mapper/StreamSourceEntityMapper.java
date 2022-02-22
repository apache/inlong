package org.apache.inlong.manager.dao.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.inlong.manager.common.pojo.source.SourceHeartbeatRequest;
import org.apache.inlong.manager.common.pojo.source.SourcePageRequest;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StreamSourceEntityMapper {

    int insert(StreamSourceEntity record);

    int insertSelective(StreamSourceEntity record);

    StreamSourceEntity selectByPrimaryKey(Integer id);

    /**
     * According to the inlong group id and inlong stream id, query the number of valid source
     *
     * @param groupId inlong group id
     * @param streamId inlong stream id
     * @return Source entity size
     */
    int selectCount(@Param("groupId") String groupId, @Param("streamId") String streamId);

    /**
     * Paging query source list based on conditions
     *
     * @param request Paging query conditions
     * @return Source entity list
     */
    List<StreamSourceEntity> selectByCondition(@Param("request") SourcePageRequest request);

    /**
     * According to the inlong group id and inlong stream id, query valid source information
     *
     * @param groupId inlong group id
     * @param streamId inlong stream id
     * @return Source entity list
     */
    List<StreamSourceEntity> selectByIdentifier(@Param("groupId") String groupId, @Param("streamId") String streamId);

    /**
     * According to the group id, stream id and source type, query valid source entity list.
     *
     * @param groupId Inlong group id.
     * @param streamId Inlong stream id.
     * @param sourceType Source type.
     * @return Source entity list.
     */
    List<StreamSourceEntity> selectByIdAndType(@Param("groupId") String groupId, @Param("streamId") String streamId,
            @Param("sourceType") String sourceType);

    /**
     * Get the distinct source type from the given groupId and streamId
     */
    List<String> selectSourceType(@Param("groupId") String groupId, @Param("streamId") String streamId);

    int updateByPrimaryKeySelective(StreamSourceEntity record);

    int updateByPrimaryKey(StreamSourceEntity record);

    int updateStatus(StreamSourceEntity entity);

    int updateHeartbeat(SourceHeartbeatRequest request);

    int deleteByPrimaryKey(Integer id);

}