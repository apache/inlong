package org.apache.inlong.manager.dao.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.inlong.manager.dao.entity.StreamTransformFieldEntity;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StreamTransformFieldEntityMapper {

    int deleteByPrimaryKey(Integer id);

    int insert(StreamTransformFieldEntity record);

    int insertSelective(StreamTransformFieldEntity record);

    /**
     * Selete undeleted transform field by transform id.
     *
     * @param transformId
     * @return
     */
    List<StreamTransformFieldEntity> selectByTransformId(@Param("transformId") Integer transformId);

    /**
     * Selete undeleted transform field by transform ids.
     *
     * @param transformIds
     * @return
     */
    List<StreamTransformFieldEntity> selectByTransformIds(@Param("transformIds") List<Integer> transformIds);

    StreamTransformFieldEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(StreamTransformFieldEntity record);

    int updateByPrimaryKey(StreamTransformFieldEntity record);

    /**
     * Insert all field list
     *
     * @param fieldList
     */
    void insertAll(@Param("list") List<StreamTransformFieldEntity> fieldList);

    /**
     * Delete all field list by transformId
     *
     * @param transformId
     * @return
     */
    int deleteAll(@Param("transformId") Integer transformId);
}