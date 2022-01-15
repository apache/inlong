package org.apache.inlong.manager.dao.mapper;

import org.apache.inlong.manager.dao.entity.TaskSinkParamsEs;

public interface TaskSinkParamsEsMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(TaskSinkParamsEs record);

    int insertSelective(TaskSinkParamsEs record);

    TaskSinkParamsEs selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(TaskSinkParamsEs record);

    int updateByPrimaryKey(TaskSinkParamsEs record);
}