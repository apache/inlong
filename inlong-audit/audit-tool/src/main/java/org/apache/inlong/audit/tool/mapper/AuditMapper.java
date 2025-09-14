/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.audit.tool.mapper;

import org.apache.inlong.audit.tool.entity.AuditMetric;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface AuditMapper {

    @Select("select inlong_group_id as inlongGroupId,inlong_stream_id as inlongStreamId, sum(count) count" +
            " from audit_data where audit_id = #{audit_id}" +
            " and log_ts between #{startLogTs} and #{endLogTs} group by inlong_group_id,inlong_stream_id")
    List<AuditMetric> getAuditMetrics(@Param("startLogTs") String startLogTs, @Param("endLogTs") String endLogTs,
            @Param("audit_id") String auditId);
}
