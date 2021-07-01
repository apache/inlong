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
import org.apache.inlong.manager.common.pojo.commonserver.CommonFileServerInfo;
import org.apache.inlong.manager.common.pojo.commonserver.CommonFileServerListVo;
import org.apache.inlong.manager.common.pojo.commonserver.CommonFileServerPageRequest;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.LoginUserUtil;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.CommonFileServerEntity;
import org.apache.inlong.manager.dao.mapper.CommonFileServerEntityMapper;
import org.apache.inlong.manager.service.core.CommonFileServerService;
import org.apache.inlong.manager.service.core.builder.CommonFileServerInfoBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class CommonFileServerServiceImpl implements CommonFileServerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonFileServerServiceImpl.class);

    @Autowired
    private CommonFileServerEntityMapper commonFileServerMapper;

    public static boolean checkStrLen(String text, int maxLength) {
        // too large.
        return text != null && text.length() > maxLength;
    }

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
    public int create(CommonFileServerInfo info) throws Exception {
        LOGGER.debug("create CommonFileServer info={} ", info);

        // Check validity
        checkValidity(info);

        // Check for duplicates based on username,ip and port
        List<CommonFileServerEntity> entities = commonFileServerMapper.selectByUsernameAndIpPort(
                info.getUsername(),
                info.getIp(),
                info.getPort());
        if (entities != null && entities.size() > 0) {
            for (CommonFileServerEntity entry : entities) {
                // Have the same normal entry
                if (entry.getIsDeleted() == 0) {
                    throw new IllegalArgumentException("Already have a CommonFileServer [" + entry.getId()
                            + "] have the same username/ip/port = [" + entry.getUsername() + "/" + entry.getIp()
                            + "/" + entry.getPort() + "]");
                }
            }
        }

        CommonFileServerEntity record = CommonBeanUtils.copyProperties(info, CommonFileServerEntity::new);
        if (record.getAccessType() == null || record.getAccessType().isEmpty()) {
            record.setAccessType("Agent");
        }

        String userName = LoginUserUtil.getLoginUserDetail().getUserName();
        record.setStatus(0);
        record.setCreator(userName);
        record.setModifier(userName);
        Date now = new Date();
        record.setCreateTime(now);
        record.setModifyTime(now);
        record.setIsDeleted(EntityStatus.UN_DELETED.getCode());

        int success = commonFileServerMapper.insert(record);
        Preconditions.checkTrue(success == 1, "insert into db failed");
        LOGGER.debug("success create CommonFileServer info=[{}] into db entry id={}", info, record.getId());
        return record.getId();
    }

    private void checkValidity(CommonFileServerInfo commonFileServerInfo) throws Exception {
        if (commonFileServerInfo.getId() > 0) {
            throw new IllegalArgumentException("CommonFileServer id = [" + commonFileServerInfo.getId()
                    + "] has already exists, please check");
        }
        if (!ipCheck(commonFileServerInfo.getIp())) {
            throw new IllegalArgumentException("CommonFileServer ip = [" + commonFileServerInfo.getIp()
                    + "] is not valid ip, please check");
        }
        checkStrFieldLength(commonFileServerInfo);
    }

    private void checkStrFieldLength(CommonFileServerInfo commonFileServerInfo) {
        if (checkStrLen(commonFileServerInfo.getAccessType(), 128)) {
            throw new IllegalArgumentException("CommonFileServer type = [" + commonFileServerInfo.getAccessType()
                    + "] length is " + commonFileServerInfo.getAccessType().length() + " and too large, "
                    + "The maximum size for the field length is 128.");
        }
        if (checkStrLen(commonFileServerInfo.getIssueType(), 128)) {
            throw new IllegalArgumentException("CommonFileServer issueType = [" + commonFileServerInfo.getIssueType()
                    + "] length is " + commonFileServerInfo.getIssueType().length() + " and too large, "
                    + "The maximum size for the field length is 128.");
        }
        if (checkStrLen(commonFileServerInfo.getUsername(), 64)) {
            throw new IllegalArgumentException("CommonFileServer username = [" + commonFileServerInfo.getUsername()
                    + "] length is " + commonFileServerInfo.getUsername().length() + " and too large, "
                    + "The maximum size for the field length is 64.");
        }
        if (checkStrLen(commonFileServerInfo.getPassword(), 64)) {
            throw new IllegalArgumentException("CommonFileServer password = [" + commonFileServerInfo.getPassword()
                    + "] length is " + commonFileServerInfo.getPassword().length() + " and too large, "
                    + "The maximum size for the field length is 64.");
        }
        if (checkStrLen(commonFileServerInfo.getVisiblePerson(), 1024)) {
            throw new IllegalArgumentException(
                    "CommonFileServer visiblePerson = [" + commonFileServerInfo.getVisiblePerson()
                            + "] length is " + commonFileServerInfo.getVisiblePerson().length() + " and too large, "
                            + "The maximum size for the field length is 1024.");
        }
        if (checkStrLen(commonFileServerInfo.getVisibleGroup(), 1024)) {
            throw new IllegalArgumentException(
                    "CommonFileServer visibleGroup = [" + commonFileServerInfo.getVisibleGroup()
                            + "] length is " + commonFileServerInfo.getVisibleGroup().length() + " and too large, "
                            + "The maximum size for the field length is 1024.");
        }
    }

    @Override
    public CommonFileServerInfo get(int id) throws Exception {
        CommonFileServerEntity entity = commonFileServerMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "CommonFileServerEntity not found by id=" + id);
        Preconditions.checkTrue(entity.getIsDeleted() == 0, "CommonFileServerEntity has been deleted, id=" + id);

        // TODO Check if it can be read
        String userName = LoginUserUtil.getLoginUserDetail().getUserName();
        if (checkVisible(userName, entity)) {
            return CommonFileServerInfoBuilder.buildFileInfoFromEntity(entity);
        } else {
            throw new IllegalArgumentException(userName + " has no right to get id=" + id
                    + ", please contact " + entity.getCreator());
        }
    }

    @Override
    public void delete(int id) throws Exception {
        String userName = LoginUserUtil.getLoginUserDetail().getUserName();
        LOGGER.info("user={} delete CommonFileServer id={} ", userName, id);

        CommonFileServerEntity entity = commonFileServerMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "CommonFileServerEntity not found by id=" + id);
        Preconditions.checkTrue(entity.getIsDeleted() == 0, "CommonFileServerEntity has been deleted, id=" + id);

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
        int success = commonFileServerMapper.updateByPrimaryKey(entity);
        Preconditions.checkTrue(success == 1, "DataBase delete id = " + id + " failed ");
        LOGGER.info("user={} success delete CommonFileServer id={}", userName, id);
    }

    @Override
    public CommonFileServerInfo update(CommonFileServerInfo commonFileServerInfo)
            throws Exception {
        String userName = LoginUserUtil.getLoginUserDetail().getUserName();
        LOGGER.info("user={} update CommonFileServer info=[{}].", userName, commonFileServerInfo);

        CommonFileServerEntity entity =
                commonFileServerMapper.selectByPrimaryKey(commonFileServerInfo.getId());
        Preconditions.checkNotNull(entity, "CommonFileServerEntity not found by id=" + commonFileServerInfo.getId());
        Preconditions.checkTrue(entity.getIsDeleted() == 0,
                "CommonFileServerEntity has been deleted, id=" + commonFileServerInfo.getId());

        // TODO Update permissions? Only those who own it can update?

        // can not update username + ip + port?
        if (commonFileServerInfo.getUsername() != null && !commonFileServerInfo.getUsername()
                .equals(entity.getUsername())) {
            throw new IllegalArgumentException(
                    entity.getId() + " username=" + entity.getUsername() + " can not be updated.");
        }
        if (commonFileServerInfo.getIp() != null && !commonFileServerInfo.getIp()
                .equals(entity.getIp())) {
            throw new IllegalArgumentException(entity.getId() + " ip=" + entity.getIp() + " can not be updated.");
        }
        if (commonFileServerInfo.getPort() != 0 && commonFileServerInfo.getPort() != entity.getPort()) {
            throw new IllegalArgumentException(entity.getId() + " port = " + entity.getPort() + " can not be updated.");
        }
        commonFileServerInfo.setPort(entity.getPort());
        // Check length
        checkStrFieldLength(commonFileServerInfo);

        if (!checkCreator(userName, entity)) {
            throw new IllegalArgumentException(userName + " is not creator, has no right to update id=" + entity.getId()
                    + ", please contact " + entity.getCreator());
        }

        CommonFileServerEntity record = new CommonFileServerEntity();
        BeanUtils.copyProperties(commonFileServerInfo, record);
        // can not update this fields.
        record.setIsDeleted(null);
        //record.setCreator(null);
        record.setCreateTime(null);

        Date now = new Date();
        record.setModifier(userName);
        record.setModifyTime(now);

        int success = commonFileServerMapper.updateByPrimaryKeySelective(record);
        Preconditions.checkTrue(success == 1, "Database update id = " + entity.getId() + " failed ");
        LOGGER.info("user={} success update CommonFileServer id={}", userName, entity.getId());

        return CommonFileServerInfoBuilder.buildFileInfoFromEntity(
                commonFileServerMapper.selectByPrimaryKey(commonFileServerInfo.getId()));
    }

    @Override
    public CommonFileServerInfo freeze(int id) throws Exception {
        String userName = LoginUserUtil.getLoginUserDetail().getUserName();
        LOGGER.info("user={} freeze CommonFileServer id=[{}].", userName, id);

        CommonFileServerEntity entity = commonFileServerMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "CommonFileServerEntity not found by id=" + id);
        Preconditions.checkTrue(entity.getIsDeleted() == 0, "CommonFileServerEntity has been deleted, id=" + id);

        // TODO There are other operations before freezing, such as stopping the agent

        if (!checkCreator(userName, entity)) {
            throw new IllegalArgumentException(userName + " is not creator, has no right to freeze id=" + id
                    + ", please contact " + entity.getCreator());
        }

        Date now = new Date();

        entity.setId(id);
        entity.setModifier(userName);
        entity.setModifyTime(now);
        entity.setStatus(1);
        int success = commonFileServerMapper.updateByPrimaryKey(entity);
        Preconditions.checkTrue(success == 1, "Database update id = " + entity.getId() + " failed ");
        LOGGER.info("user={} success freeze CommonFileServer id={}", userName, entity.getId());

        return CommonFileServerInfoBuilder.buildFileInfoFromEntity(
                commonFileServerMapper.selectByPrimaryKey(id));
    }

    @Override
    public CommonFileServerInfo unfreeze(int id) throws Exception {
        String userName = LoginUserUtil.getLoginUserDetail().getUserName();
        LOGGER.info("user={} unfreeze CommonFileServer id=[{}].", userName, id);

        CommonFileServerEntity entity = commonFileServerMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "CommonFileServerEntity not found by id=" + id);
        Preconditions.checkTrue(entity.getIsDeleted() == 0, "CommonFileServerEntity has been deleted, id=" + id);

        // TODO There are other operations before thawing, such as starting the agent

        if (!checkCreator(userName, entity)) {
            throw new IllegalArgumentException(userName + " is not creator, has no right to unfreeze id=" + id
                    + ", please contact " + entity.getCreator());
        }

        Date now = new Date();

        entity.setId(id);
        entity.setModifier(userName);
        entity.setModifyTime(now);
        entity.setStatus(0);
        int success = commonFileServerMapper.updateByPrimaryKey(entity);
        Preconditions.checkTrue(success == 1, "DataBase update id = " + entity.getId() + " failed ");
        LOGGER.info("user={} success unfreeze CommonFileServer id={}", userName, entity.getId());

        return CommonFileServerInfoBuilder.buildFileInfoFromEntity(
                commonFileServerMapper.selectByPrimaryKey(id));
    }

    @Override
    public List<CommonFileServerInfo> getByUser(String user) throws Exception {
        List<CommonFileServerEntity> all = commonFileServerMapper.selectAll();

        // Get group information of user
        List<String> groups = getUserGroups(user);
        //LOGGER.info("user = " + user + ", groups =  " + groups);

        List<CommonFileServerInfo> results = new ArrayList<>();

        Splitter commaSplitter = Splitter.on(',');
        for (CommonFileServerEntity entry : all) {
            if (entry.getCreator().equals(user)) {
                results.add(CommonFileServerInfoBuilder.buildFileInfoFromEntity(entry));
                continue;
            }
            // check user relative entry
            List<String> vPersion = commaSplitter.splitToList(entry.getVisiblePerson());
            List<String> vGroup = commaSplitter.splitToList(entry.getVisibleGroup());
            if (checkUserVisible(user, groups, vPersion, vGroup)) {
                results.add(CommonFileServerInfoBuilder.buildFileInfoFromEntity(entry));
            }
        }
        return results;
    }

    private boolean checkUserVisible(String user, List<String> groups, List<String> vPersion,
            List<String> vGroup) {
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
    public CommonFileServerInfo addVisiblePerson(Integer id, String visiblePerson) {
        String username = LoginUserUtil.getLoginUserDetail().getUserName();
        LOGGER.info("user={}, add visible person, id={}, visible group={}", username, id, visiblePerson);

        CommonFileServerEntity entity = commonFileServerMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "CommonFileServerEntity not found by id=" + id);
        Preconditions.checkTrue(entity.getIsDeleted() == 0,
                "CommonFileServerEntity has been deleted, id=" + id);

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

        int success = commonFileServerMapper.updateByPrimaryKey(entity);
        Preconditions.checkTrue(success == 1, "DataBase update for id = " + entity.getId() + " failed ");
        LOGGER.info("user={} success addVisiblePerson for CommonFileServer id={}", username, entity.getId());

        return CommonFileServerInfoBuilder.buildFileInfoFromEntity(commonFileServerMapper.selectByPrimaryKey(id));
    }

    @Override
    public CommonFileServerInfo deleteVisiblePerson(Integer id, String visiblePerson) {
        String username = LoginUserUtil.getLoginUserDetail().getUserName();
        LOGGER.info("user={}, delete visible person, id={}, visible group={}", username, id, visiblePerson);

        CommonFileServerEntity entity =
                commonFileServerMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "CommonFileServerEntity not found by id=" + id);
        Preconditions.checkTrue(entity.getIsDeleted() == 0, "CommonFileServerEntity has been deleted, id=" + id);

        if (!checkCreator(username, entity)) {
            throw new IllegalArgumentException(
                    username + " is not creator, has no right to deleteVisiblePerson id=" + entity.getId()
                            + ", please contact " + entity.getCreator());
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
        int success = commonFileServerMapper.updateByPrimaryKey(entity);

        Preconditions.checkTrue(success == 1, "DataBase update for id = " + entity.getId() + " failed ");
        LOGGER.info("user={} success deleteVisiblePerson for CommonFileServer id={}", username, entity.getId());

        return CommonFileServerInfoBuilder.buildFileInfoFromEntity(
                commonFileServerMapper.selectByPrimaryKey(id));
    }

    @Override
    public CommonFileServerInfo addVisibleGroup(Integer id, String visibleGroup) {
        String username = LoginUserUtil.getLoginUserDetail().getUserName();
        LOGGER.info("user={}, add visible group, id={}, visible group={}", username, id, visibleGroup);

        CommonFileServerEntity entity = commonFileServerMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "CommonFileServerEntity not found by id=" + id);
        Preconditions.checkTrue(entity.getIsDeleted() == 0, "CommonFileServerEntity has been deleted, id=" + id);

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
        int success = commonFileServerMapper.updateByPrimaryKey(entity);

        Preconditions.checkTrue(success == 1, "DataBase update for id = " + entity.getId() + " failed ");
        LOGGER.info("user={} success addVisibleGroup for CommonFileServer id={}", username, entity.getId());

        return CommonFileServerInfoBuilder.buildFileInfoFromEntity(commonFileServerMapper.selectByPrimaryKey(id));
    }

    @Override
    public CommonFileServerInfo deleteVisibleGroup(Integer id, String visibleGroup) {
        String username = LoginUserUtil.getLoginUserDetail().getUserName();
        LOGGER.info("user={}, delete visible group, id={}, visible group={}", username, id, visibleGroup);

        CommonFileServerEntity entity =
                commonFileServerMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, "CommonFileServerEntity not found by id=" + id);
        Preconditions.checkTrue(entity.getIsDeleted() == 0, "CommonFileServerEntity has been deleted, id=" + id);

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
        int success = commonFileServerMapper.updateByPrimaryKey(entity);

        Preconditions.checkTrue(success == 1, "DataBase update for id = " + entity.getId() + " failed ");
        LOGGER.info("user={} success deleteVisibleGroup for CommonFileServer id={}", username, entity.getId());

        return CommonFileServerInfoBuilder.buildFileInfoFromEntity(
                commonFileServerMapper.selectByPrimaryKey(id));
    }

    @Override
    public PageInfo<CommonFileServerListVo> listByCondition(CommonFileServerPageRequest request)
            throws Exception {
        String username = LoginUserUtil.getLoginUserDetail().getUserName();

        request.setCurrentUser(username);
        LOGGER.debug("{} begin to list CommonFileServer info by {}", username, request);

        // Get groups info for user.
        List<String> groups = getUserGroups(username);
        request.setUserGroups(groups);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<CommonFileServerEntity> pageResult =
                (Page<CommonFileServerEntity>) commonFileServerMapper.selectByCondition(request);
        PageInfo<CommonFileServerListVo> pageInfo = pageResult.toPageInfo(
                entity -> CommonBeanUtils.copyProperties(entity, CommonFileServerListVo::new));

        pageInfo.setTotal(pageResult.getTotal());
        return pageInfo;
    }

    private List<String> getUserGroups(String userName) {
        return new ArrayList<>();
    }

    private boolean checkVisible(String userName, CommonFileServerEntity entity) {
        boolean passed = false;
        // check creator
        passed = passed || checkCreator(userName, entity);
        if (!passed) {
            // check visiblePerson
            passed = passed || checkVisiblePerson(userName, entity);
            if (!passed) {
                // check visibleGroup
                passed = passed || checkVisibleGroup(userName, entity);
            }
        }
        return passed;
    }

    private boolean checkCreator(String username, CommonFileServerEntity entity) {
        return entity.getCreator().equals(username);
    }

    private boolean checkVisiblePerson(String username, CommonFileServerEntity entity) {
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

    private boolean checkVisibleGroup(String username, CommonFileServerEntity entity) {
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
