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

package org.apache.inlong.manager.service.core.impl;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.pojo.commonserver.CommonDbServerInfo;
import org.apache.inlong.manager.common.pojo.commonserver.CommonDbServerListVo;
import org.apache.inlong.manager.common.pojo.commonserver.CommonDbServerPageRequest;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.LoginUserUtil;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.CommonDbServerEntity;
import org.apache.inlong.manager.dao.mapper.CommonDbServerEntityMapper;
import org.apache.inlong.manager.service.core.CommonDBServerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class CommonDBServerServiceImpl implements CommonDBServerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonDBServerServiceImpl.class);

    @Autowired
    private CommonDbServerEntityMapper commonDbServerMapper;

    public static boolean checkStrLen(String text, int maxLength) {
        if (text != null && text.length() > maxLength) {
            // too large.
            return true;
        }
        return false;
    }

    /**
     * Check if the IP string is valid
     *
     * @param text IP string
     * @return true or false
     */
    public static boolean ipCheck(String text) {
        if (text != null && !text.isEmpty()) {
            // Define regular expression
            String regex = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."
                    + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                    + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                    + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$";
            // Determine whether the ip address matches the regular expression
            return text.matches(regex);
        }
        return false;
    }

    @Override
    public int save(CommonDbServerInfo info) throws Exception {
        LOGGER.debug("create CommonDbServer info=[{}]", info);

        // Check validity
        checkValidity(info);

        // Check for duplicates based on username, dbType, dbServerIp and port
        List<CommonDbServerEntity> entities = commonDbServerMapper.selectByUsernameAndIpPort(
                info.getUsername(),
                info.getDbType(),
                info.getDbServerIp(),
                info.getPort());
        if (entities != null && entities.size() > 0) {
            for (CommonDbServerEntity entry : entities) {
                // Have the same normal entry
                if (entry.getIsDeleted() == 0) {
                    throw new IllegalArgumentException("Already have a CommonDbServer [" + entry.getId()
                            + "] have the same username/dbType/dbServerIp/port = [" + entry.getUsername() + "/"
                            + entry.getDbType() + "/" + entry.getDbServerIp() + "/" + entry.getPort()
                            + "]");
                }
            }
        }

        CommonDbServerEntity record = CommonBeanUtils.copyProperties(info, CommonDbServerEntity::new);
        record.setStatus(0);
        String userName = LoginUserUtil.getLoginUserDetail().getUserName();
        record.setCreator(userName);
        record.setModifier(userName);
        Date now = new Date();
        record.setCreateTime(now);
        record.setModifyTime(now);
        record.setIsDeleted(EntityStatus.UN_DELETED.getCode());

        int success = commonDbServerMapper.insert(record);
        Preconditions.checkTrue(success == 1, "insert into db failed");
        LOGGER.debug("success create CommonDbServer info=[{}] into db entry id={}", info, record.getId());
        return record.getId();
    }

    private void checkValidity(CommonDbServerInfo commonDbServerInfo) throws Exception {
        if (commonDbServerInfo.getId() > 0) {
            throw new IllegalArgumentException("CommonDbServer id [" + commonDbServerInfo.getId()
                    + "] has already exists, please check");
        }
        if (!ipCheck(commonDbServerInfo.getDbServerIp())) {
            throw new IllegalArgumentException("CommonDbServer dbServerIp = [" + commonDbServerInfo.getDbServerIp()
                    + "] is not valid ip, please check");
        }
        checkStrFieldLength(commonDbServerInfo);
    }

    private void checkStrFieldLength(CommonDbServerInfo commonDbServerInfo) throws Exception {
        if (checkStrLen(commonDbServerInfo.getConnectionName(), 128)) {
            throw new IllegalArgumentException(
                    "CommonDbServerInfo connectionName = [" + commonDbServerInfo.getConnectionName()
                            + "] length is " + commonDbServerInfo.getConnectionName().length() + " and too large, "
                            + "The maximum size for the field length is 128.");
        }
        if (checkStrLen(commonDbServerInfo.getDbType(), 128)) {
            throw new IllegalArgumentException("CommonDbServerInfo dbType = [" + commonDbServerInfo.getDbType()
                    + "] length is " + commonDbServerInfo.getDbType().length() + " and too large, "
                    + "The maximum size for the field length is 128.");
        }
        if (checkStrLen(commonDbServerInfo.getDbName(), 128)) {
            throw new IllegalArgumentException("CommonDbServerInfo dbName = [" + commonDbServerInfo.getDbName()
                    + "] length is " + commonDbServerInfo.getDbName().length() + " and too large, "
                    + "The maximum size for the field length is 128.");
        }
        if (checkStrLen(commonDbServerInfo.getUsername(), 64)) {
            throw new IllegalArgumentException("CommonDbServerInfo username = [" + commonDbServerInfo.getUsername()
                    + "] length is " + commonDbServerInfo.getUsername().length() + " and too large, "
                    + "The maximum size for the field length is 64.");
        }
        if (checkStrLen(commonDbServerInfo.getPassword(), 64)) {
            throw new IllegalArgumentException("CommonDbServerInfo password = [" + commonDbServerInfo.getPassword()
                    + "] length is " + commonDbServerInfo.getPassword().length() + " and too large, "
                    + "The maximum size for the field length is 64.");
        }
        if (checkStrLen(commonDbServerInfo.getInCharges(), 512)) {
            throw new IllegalArgumentException("CommonDbServerInfo inCharges = [" + commonDbServerInfo.getInCharges()
                    + "] length is " + commonDbServerInfo.getInCharges().length() + " and too large, "
                    + "The maximum size for the field length is 512.");
        }
        if (checkStrLen(commonDbServerInfo.getDbDescription(), 256)) {
            throw new IllegalArgumentException(
                    "CommonDbServerInfo dbDescription = [" + commonDbServerInfo.getDbDescription()
                            + "] length is " + commonDbServerInfo.getDbDescription().length() + " and too large, "
                            + "The maximum size for the field length is 256.");
        }
        if (checkStrLen(commonDbServerInfo.getVisiblePerson(), 1024)) {
            throw new IllegalArgumentException(
                    "CommonDbServerInfo visiblePerson = [" + commonDbServerInfo.getVisiblePerson()
                            + "] length is " + commonDbServerInfo.getVisiblePerson().length() + " and too large, "
                            + "The maximum size for the field length is 1024.");
        }
        if (checkStrLen(commonDbServerInfo.getVisibleGroup(), 1024)) {
            throw new IllegalArgumentException(
                    "CommonDbServerInfo visibleGroup = [" + commonDbServerInfo.getVisibleGroup()
                            + "] length is " + commonDbServerInfo.getVisibleGroup().length() + " and too large, "
                            + "The maximum size for the field length is 1024.");
        }
    }

    @Override
    public CommonDbServerInfo get(int id) throws Exception {
        CommonDbServerEntity entity = commonDbServerMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "CommonDbServerEntity not found by id=" + id);
        Preconditions.checkTrue(entity.getIsDeleted() == 0, "CommonDbServerEntity has been deleted, id=" + id);

        String userName = LoginUserUtil.getLoginUserDetail().getUserName();
        if (checkVisible(userName, entity)) {
            return CommonBeanUtils.copyProperties(entity, CommonDbServerInfo::new);
        } else {
            throw new IllegalArgumentException(userName + " has no right to get id=" + id
                    + ", please contact " + entity.getCreator());
        }
    }

    @Override
    public void delete(int id) throws Exception {
        String userName = LoginUserUtil.getLoginUserDetail().getUserName();
        LOGGER.info("user={} delete CommonDbServerInfo id=[{}]", userName, id);

        CommonDbServerEntity entity = commonDbServerMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "CommonDbServerEntity not found by id=" + id);
        Preconditions.checkTrue(entity.getIsDeleted() == 0, "CommonDbServerEntity has been deleted, id=" + id);

        // TODO Check if it is quoted, only those that no one quotes can be deleted

        if (!checkCreator(userName, entity)) {
            throw new IllegalArgumentException(userName + " is not creator, has no right to delete id=" + id
                    + ", please contact " + entity.getCreator());
        }

        // Mark deletion
        Date now = new Date();
        entity.setIsDeleted(1);
        entity.setModifier(userName);
        entity.setModifyTime(now);

        int success = commonDbServerMapper.updateByPrimaryKey(entity);
        Preconditions.checkTrue(success == 1, "DataBase delete id = " + id + " failed ");
        LOGGER.info("user={} success delete CommonDbServer id={}", userName, id);
    }

    @Override
    public CommonDbServerInfo update(CommonDbServerInfo serverInfo) throws Exception {
        String userName = LoginUserUtil.getLoginUserDetail().getUserName();
        LOGGER.info("user={} update CommonDbServerInfo info=[{}]", userName, serverInfo);
        CommonDbServerEntity entity = commonDbServerMapper.selectByPrimaryKey(serverInfo.getId());
        Preconditions.checkNotNull(entity, "CommonDbServerEntity not found by id=" + serverInfo.getId());
        Preconditions.checkTrue(entity.getIsDeleted() == 0,
                "CommonDbServerEntity has been deleted, id=" + serverInfo.getId());

        // can not update username + dbType + dbServerIp + port?
        if (serverInfo.getUsername() != null && !serverInfo.getUsername()
                .equals(entity.getUsername())) {
            throw new IllegalArgumentException(
                    entity.getId() + " username=" + entity.getUsername() + " can not be updated.");
        }
        if (serverInfo.getDbType() != null && !serverInfo.getDbType()
                .equals(entity.getDbType())) {
            throw new IllegalArgumentException(
                    entity.getId() + " dbType=" + entity.getDbType() + " can not be updated.");
        }
        if (serverInfo.getDbServerIp() != null && !serverInfo.getDbServerIp()
                .equals(entity.getDbServerIp())) {
            throw new IllegalArgumentException(
                    entity.getId() + " dbServerIp=" + entity.getDbServerIp() + " can not be updated.");
        }
        if (serverInfo.getPort() != 0 && serverInfo.getPort() != entity.getPort()) {
            throw new IllegalArgumentException(entity.getId() + " port = " + entity.getPort() + " can not be updated.");
        }
        serverInfo.setPort(entity.getPort());
        checkStrFieldLength(serverInfo);

        if (!checkCreator(userName, entity)) {
            throw new IllegalArgumentException(userName + " is not creator, has no right to update id=" + entity.getId()
                    + ", please contact " + entity.getCreator());
        }

        CommonDbServerEntity record = CommonBeanUtils.copyProperties(serverInfo, CommonDbServerEntity::new);
        record.setModifier(userName);
        record.setModifyTime(new Date());

        int success = commonDbServerMapper.updateByPrimaryKeySelective(record);
        Preconditions.checkTrue(success == 1, "Database update id = " + entity.getId() + " failed ");
        LOGGER.info("user={} success update CommonDbServer id={}", userName, entity.getId());

        return CommonBeanUtils.copyProperties(record, CommonDbServerInfo::new);
    }

    @Override
    public List<CommonDbServerInfo> getByUser(String user) throws Exception {
        List<CommonDbServerEntity> all = commonDbServerMapper.selectAll();

        // Get group information of user
        List<String> groups = getUserGroups(user);

        List<CommonDbServerInfo> results = new ArrayList<>();

        Splitter commaSplitter = Splitter.on(',');
        for (CommonDbServerEntity entity : all) {
            if (entity.getCreator().equals(user)) {
                results.add(CommonBeanUtils.copyProperties(entity, CommonDbServerInfo::new));
                continue;
            }
            // check user relative entry
            List<String> inChargeSplits = commaSplitter.splitToList(entity.getInCharges());
            List<String> vPersion = commaSplitter.splitToList(entity.getVisiblePerson());
            List<String> vGroup = commaSplitter.splitToList(entity.getVisibleGroup());
            if (checkUserVisible(user, groups, inChargeSplits, vPersion, vGroup)) {
                results.add(CommonBeanUtils.copyProperties(entity, CommonDbServerInfo::new));
            }
        }
        return results;
    }

    private boolean checkUserVisible(String user,
            List<String> groups,
            List<String> inCharges,
            List<String> vPersion,
            List<String> vGroup) {
        for (String entry : inCharges) {
            if (entry.equals(user)) {
                return true;
            }
        }
        for (String persion : vPersion) {
            if (persion.equals(user)) {
                return true;
            }
        }
        for (String group : vGroup) {
            for (String g : groups) {
                if (group.equals(g)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public CommonDbServerInfo addVisiblePerson(Integer id, String visiblePerson) {
        String username = LoginUserUtil.getLoginUserDetail().getUserName();
        LOGGER.info("user={}, add visible person, id={}, visible group={}", username, id, visiblePerson);

        CommonDbServerEntity entity = commonDbServerMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "CommonDbServerEntity not found by id=" + id);
        Preconditions.checkTrue(entity.getIsDeleted() == 0, "CommonDbServerEntity has been deleted, id=" + id);

        if (!checkCreator(username, entity)) {
            throw new IllegalArgumentException(username + " is not creator, has no right to addVisiblePerson id="
                    + entity.getId() + ", please contact " + entity.getCreator());
        }

        // add user to visiblePerson
        Splitter commaSplitter = Splitter.on(',').omitEmptyStrings().trimResults();
        List<String> adds = commaSplitter.splitToList(visiblePerson);
        List<String> alreadyExists = commaSplitter.splitToList(entity.getVisiblePerson());
        HashSet<String> all = new HashSet<>();
        all.addAll(alreadyExists);
        all.addAll(adds);

        Joiner joiner = Joiner.on(",").skipNulls();
        String result = joiner.join(all);
        entity.setVisiblePerson(result);

        if (checkStrLen(entity.getVisiblePerson(), 1024)) {
            throw new IllegalArgumentException("CommonFileServer visiblePerson = [" + entity.getVisiblePerson()
                    + "] length is " + entity.getVisiblePerson().length() + " and too large, "
                    + "The maximum size for the field length is 1024.");
        }

        Date now = new Date();
        entity.setModifier(username);
        entity.setModifyTime(now);

        int success = commonDbServerMapper.updateByPrimaryKey(entity);
        Preconditions.checkTrue(success == 1, "DataBase update for id = " + entity.getId() + " failed ");
        LOGGER.info("user={} success addVisiblePerson for CommonDbServer id={}", username, entity.getId());

        return CommonBeanUtils.copyProperties(entity, CommonDbServerInfo::new);
    }

    @Override
    public CommonDbServerInfo deleteVisiblePerson(Integer id, String visiblePerson) {
        String username = LoginUserUtil.getLoginUserDetail().getUserName();
        LOGGER.info("user={}, delete visible group, id={}, visible group={}", username, id, visiblePerson);

        CommonDbServerEntity entity = commonDbServerMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "CommonDbServerEntity not found by id=" + id);
        Preconditions.checkTrue(entity.getIsDeleted() == 0, "CommonDbServerEntity has been deleted, id=" + id);

        if (!checkCreator(username, entity)) {
            throw new IllegalArgumentException(username + " is not creator, has no right to deleteVisiblePerson id="
                    + entity.getId() + ", please contact " + entity.getCreator());
        }

        // delete user from visiblePerson
        Splitter commaSplitter = Splitter.on(',').omitEmptyStrings().trimResults();
        List<String> removes = commaSplitter.splitToList(visiblePerson);
        List<String> alreadyExists = commaSplitter.splitToList(entity.getVisiblePerson());
        HashSet<String> all = new HashSet<>(alreadyExists);
        removes.forEach(all::remove);

        Joiner joiner = Joiner.on(",").skipNulls();
        String result = joiner.join(all);
        entity.setVisiblePerson(result);

        Date now = new Date();
        entity.setModifier(username);
        entity.setModifyTime(now);

        int success = commonDbServerMapper.updateByPrimaryKey(entity);
        Preconditions.checkTrue(success == 1, "DataBase update for id = " + entity.getId() + " failed ");
        LOGGER.info("user={} success deleteVisiblePerson for CommonDbServer id={}", username, entity.getId());

        return CommonBeanUtils.copyProperties(entity, CommonDbServerInfo::new);
    }

    @Override
    public CommonDbServerInfo addVisibleGroup(Integer id, String visibleGroup) {
        String username = LoginUserUtil.getLoginUserDetail().getUserName();
        LOGGER.info("user={}, add visible group, id={}, visible group={}", username, id, visibleGroup);

        CommonDbServerEntity entity = commonDbServerMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "CommonDbServerEntity not found by id=" + id);
        Preconditions.checkTrue(entity.getIsDeleted() == 0, "CommonDbServerEntity has been deleted, id=" + id);

        if (!checkCreator(username, entity)) {
            throw new IllegalArgumentException(username + " is not creator, has no right to addVisibleGroup id="
                    + entity.getId() + ", please contact " + entity.getCreator());
        }

        // add group to visibleGroup
        Splitter commaSplitter = Splitter.on(',').omitEmptyStrings().trimResults();
        List<String> adds = commaSplitter.splitToList(visibleGroup);
        List<String> alreadyExists = commaSplitter.splitToList(entity.getVisibleGroup());
        HashSet<String> all = new HashSet<>();
        all.addAll(alreadyExists);
        all.addAll(adds);

        Joiner joiner = Joiner.on(",").skipNulls();
        String result = joiner.join(all);
        entity.setVisibleGroup(result);

        if (checkStrLen(entity.getVisibleGroup(), 1024)) {
            throw new IllegalArgumentException("CommonFileServer visibleGroup = [" + entity.getVisibleGroup()
                    + "] length is " + entity.getVisibleGroup().length() + " and too large, "
                    + "The maximum size for the field length is 1024.");
        }

        Date now = new Date();
        entity.setModifier(username);
        entity.setModifyTime(now);

        int success = commonDbServerMapper.updateByPrimaryKey(entity);
        Preconditions.checkTrue(success == 1, "DataBase update for id = " + entity.getId() + " failed ");
        LOGGER.info("user={} success addVisibleGroup for CommonDbServer id={}", username, entity.getId());

        return CommonBeanUtils.copyProperties(entity, CommonDbServerInfo::new);
    }

    @Override
    public CommonDbServerInfo deleteVisibleGroup(Integer id, String visibleGroup) {
        String username = LoginUserUtil.getLoginUserDetail().getUserName();
        LOGGER.info("user={}, delete visible group, id={}, visible group={}", username, id, visibleGroup);

        CommonDbServerEntity entity =
                commonDbServerMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "CommonDbServerEntity not found by id=" + id);
        Preconditions.checkTrue(entity.getIsDeleted() == 0, "CommonDbServerEntity has been deleted, id=" + id);

        if (!checkCreator(username, entity)) {
            throw new IllegalArgumentException(username + " is not creator, has no right to deleteVisibleGroup id="
                    + entity.getId() + ", please contact " + entity.getCreator());
        }

        // delete group from visibleGroup
        Splitter commaSplitter = Splitter.on(',').omitEmptyStrings().trimResults();
        List<String> removes = commaSplitter.splitToList(visibleGroup);
        List<String> alreadyExists = commaSplitter.splitToList(entity.getVisibleGroup());
        HashSet<String> all = new HashSet<>(alreadyExists);
        removes.forEach(all::remove);

        Joiner joiner = Joiner.on(",").skipNulls();
        String result = joiner.join(all);
        entity.setVisibleGroup(result);

        Date now = new Date();
        entity.setModifier(username);
        entity.setModifyTime(now);
        int success = commonDbServerMapper.updateByPrimaryKey(entity);

        Preconditions.checkTrue(success == 1, "DataBase update for id = " + entity.getId() + " failed ");
        LOGGER.info("user={} success deleteVisibleGroup for CommonDbServer id={}", username, entity.getId());

        return CommonBeanUtils.copyProperties(entity, CommonDbServerInfo::new);
    }

    @Override
    public PageInfo<CommonDbServerListVo> listByCondition(CommonDbServerPageRequest request)
            throws Exception {
        String username = LoginUserUtil.getLoginUserDetail().getUserName();
        LOGGER.debug("{} begin to list CommonDbServer info by {}", username, request);
        request.setCurrentUser(username);

        // Get groups info for user.
        List<String> groups = getUserGroups(username);
        request.setUserGroups(groups);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<CommonDbServerEntity> entityPage =
                (Page<CommonDbServerEntity>) commonDbServerMapper.selectByCondition(request);
        List<CommonDbServerListVo> dbServerList =
                CommonBeanUtils.copyListProperties(entityPage, CommonDbServerListVo::new);
        // Encapsulate the paging query results into the PageInfo object to obtain related paging information
        PageInfo<CommonDbServerListVo> page = new PageInfo<>(dbServerList);
        page.setTotal(entityPage.getTotal());

        LOGGER.info("success to list CommonFileServer info");
        return page;
    }

    private List<String> getUserGroups(String userName) {
        return new ArrayList<>();
    }

    private boolean checkVisible(String userName, CommonDbServerEntity entity) {
        boolean passed = false;
        // check creator
        passed = checkCreator(userName, entity);
        if (!passed) {
            // check visiblePerson
            passed = checkVisiblePerson(userName, entity);
            if (!passed) {
                // check visibleGroup
                passed = checkVisibleGroup(userName, entity);
            }
        }
        return passed;
    }

    private boolean checkCreator(String username, CommonDbServerEntity entity) {
        return entity.getCreator().equals(username);
    }

    private boolean checkVisiblePerson(String username, CommonDbServerEntity entity) {
        return contains(username, entity.getVisiblePerson());
    }

    private boolean contains(String name, String visibles) {
        if (visibles == null || visibles.isEmpty()) {
            return false;
        }
        Splitter commaSplitter = Splitter.on(',').omitEmptyStrings().trimResults();
        for (String entry : commaSplitter.split(visibles)) {
            if (entry.equals(name)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkVisibleGroup(String username, CommonDbServerEntity entity) {
        String visibleGroup = entity.getVisibleGroup();
        if (visibleGroup == null || visibleGroup.isEmpty()) {
            return false;
        }
        List<String> groups = getUserGroups(username);
        for (String group : groups) {
            if (contains(group, visibleGroup)) {
                return true;
            }
        }
        return false;
    }
}
