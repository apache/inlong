package org.apache.inlong.manager.dao.mapper;

import org.apache.inlong.manager.dao.entity.SortClusterConfig;

public interface SortClusterConfigMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(SortClusterConfig record);

    int insertSelective(SortClusterConfig record);

    SortClusterConfig selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(SortClusterConfig record);

    int updateByPrimaryKey(SortClusterConfig record);
}