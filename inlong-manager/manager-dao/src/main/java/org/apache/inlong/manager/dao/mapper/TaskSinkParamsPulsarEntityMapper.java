package org.apache.inlong.manager.dao.mapper;

import org.apache.inlong.manager.dao.entity.TaskSinkParamsPulsarEntity;

public interface TaskSinkParamsPulsarEntityMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(TaskSinkParamsPulsarEntity record);

    int insertSelective(TaskSinkParamsPulsarEntity record);

    TaskSinkParamsPulsarEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(TaskSinkParamsPulsarEntity record);

    int updateByPrimaryKey(TaskSinkParamsPulsarEntity record);
}