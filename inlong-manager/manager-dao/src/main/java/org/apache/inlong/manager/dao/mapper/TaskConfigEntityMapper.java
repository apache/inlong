package org.apache.inlong.manager.dao.mapper;

import org.apache.inlong.manager.dao.entity.TaskConfigEntity;

public interface TaskConfigEntityMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(TaskConfigEntity record);

    int insertSelective(TaskConfigEntity record);

    TaskConfigEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(TaskConfigEntity record);

    int updateByPrimaryKey(TaskConfigEntity record);
}