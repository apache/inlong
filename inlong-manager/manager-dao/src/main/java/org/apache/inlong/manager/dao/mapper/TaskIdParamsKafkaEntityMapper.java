package org.apache.inlong.manager.dao.mapper;

import org.apache.inlong.manager.dao.entity.TaskIdParamsKafkaEntity;

public interface TaskIdParamsKafkaEntityMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(TaskIdParamsKafkaEntity record);

    int insertSelective(TaskIdParamsKafkaEntity record);

    TaskIdParamsKafkaEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(TaskIdParamsKafkaEntity record);

    int updateByPrimaryKey(TaskIdParamsKafkaEntity record);
}