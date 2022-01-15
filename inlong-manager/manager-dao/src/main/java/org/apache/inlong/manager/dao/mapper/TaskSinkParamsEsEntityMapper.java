package org.apache.inlong.manager.dao.mapper;

import org.apache.inlong.manager.dao.entity.TaskSinkParamsEsEntity;

public interface TaskSinkParamsEsEntityMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(TaskSinkParamsEsEntity record);

    int insertSelective(TaskSinkParamsEsEntity record);

    TaskSinkParamsEsEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(TaskSinkParamsEsEntity record);

    int updateByPrimaryKey(TaskSinkParamsEsEntity record);
}