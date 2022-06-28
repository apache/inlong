package org.apache.inlong.manager.dao.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.inlong.manager.common.pojo.cluster.ClusterTagPageRequest;
import org.apache.inlong.manager.dao.entity.InlongClusterTagEntity;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface InlongClusterTagEntityMapper {

    int insert(InlongClusterTagEntity record);

    InlongClusterTagEntity selectById(Integer id);

    InlongClusterTagEntity selectByTag(@Param("clusterTag") String clusterTag);

    List<InlongClusterTagEntity> selectByCondition(ClusterTagPageRequest request);

    int updateById(InlongClusterTagEntity record);

    int updateByPrimaryKeySelective(InlongClusterTagEntity record);

    int deleteByPrimaryKey(Integer id);

}
