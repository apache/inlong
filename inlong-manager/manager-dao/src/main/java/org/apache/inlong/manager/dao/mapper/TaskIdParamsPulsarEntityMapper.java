package org.apache.inlong.manager.dao.mapper;

import org.apache.inlong.manager.dao.entity.TaskIdParamsPulsarEntity;

public interface TaskIdParamsPulsarEntityMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(TaskIdParamsPulsarEntity record);

    int insertSelective(TaskIdParamsPulsarEntity record);

    TaskIdParamsPulsarEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(TaskIdParamsPulsarEntity record);

    int updateByPrimaryKey(TaskIdParamsPulsarEntity record);
}