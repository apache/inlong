package org.apache.inlong.manager.dao.mapper;

import org.apache.inlong.manager.dao.entity.TaskSinkParamsKafkaEntity;

public interface TaskSinkParamsKafkaEntityMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(TaskSinkParamsKafkaEntity record);

    int insertSelective(TaskSinkParamsKafkaEntity record);

    TaskSinkParamsKafkaEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(TaskSinkParamsKafkaEntity record);

    int updateByPrimaryKey(TaskSinkParamsKafkaEntity record);
}