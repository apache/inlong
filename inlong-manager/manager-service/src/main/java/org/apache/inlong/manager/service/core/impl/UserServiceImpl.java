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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.UserTypeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.user.UserDetailListVO;
import org.apache.inlong.manager.common.pojo.user.UserDetailPageRequest;
import org.apache.inlong.manager.common.pojo.user.UserInfo;
import org.apache.inlong.manager.common.util.AESUtils;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.DateUtils;
import org.apache.inlong.manager.common.util.LoginUserUtils;
import org.apache.inlong.manager.common.util.MD5Utils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.common.util.RSAUtils;
import org.apache.inlong.manager.dao.entity.UserEntity;
import org.apache.inlong.manager.dao.entity.UserEntityExample;
import org.apache.inlong.manager.dao.entity.UserEntityExample.Criteria;
import org.apache.inlong.manager.dao.mapper.UserEntityMapper;
import org.apache.inlong.manager.service.core.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * User service layer implementation
 */
@Slf4j
@Service
public class UserServiceImpl implements UserService {

    @Autowired
    private UserEntityMapper userMapper;

    @Override
    public UserEntity getByUsername(String username) {
        UserEntityExample example = new UserEntityExample();
        example.createCriteria().andNameEqualTo(username);
        List<UserEntity> list = userMapper.selectByExample(example);
        return list.isEmpty() ? null : list.get(0);
    }

    @Override
    public UserInfo getById(Integer userId, String currentUser) {
        Preconditions.checkNotNull(userId, "User id should not be empty");
        UserEntity entity = userMapper.selectByPrimaryKey(userId);
        UserEntity curUser = getByUsername(currentUser);
        Preconditions.checkNotNull(entity, "User not exists with id " + userId);
        Preconditions.checkTrue(curUser.getAccountType().equals(UserTypeEnum.ADMIN.getCode())
                        || Objects.equals(entity.getName(), currentUser),
                "current user does not have permission to get other users info");

        UserInfo result = new UserInfo();
        result.setId(entity.getId());
        result.setUsername(entity.getName());
        result.setValidDays(DateUtils.getValidDays(entity.getCreateTime(), entity.getDueDate()));
        result.setType(entity.getAccountType());

        if (StringUtils.isNotBlank(entity.getSecretKey()) && StringUtils.isNotBlank(entity.getPublicKey())) {
            try {
                // decipher according to stored key version
                // note that if the version is null then the string is treated as unencrypted plain text
                Integer version = entity.getEncryptVersion();
                byte[] secretKeyBytes = AESUtils.decryptAsString(entity.getSecretKey(), version);
                byte[] publicKeyBytes = AESUtils.decryptAsString(entity.getPublicKey(), version);
                result.setSecretKey(new String(secretKeyBytes, StandardCharsets.UTF_8));
                result.setPublicKey(new String(publicKeyBytes, StandardCharsets.UTF_8));
            } catch (Exception e) {
                String errMsg = String.format("decryption error: %s", e.getMessage());
                log.error(errMsg, e);
                throw new BusinessException(errMsg);
            }
        }

        log.debug("success to get user info by id={}", userId);
        return result;
    }

    @Override
    public boolean create(UserInfo userInfo) {
        String username = userInfo.getUsername();
        UserEntity userExists = getByUsername(username);
        Preconditions.checkNull(userExists, "username [" + username + "] already exists");

        UserEntity entity = new UserEntity();
        entity.setAccountType(userInfo.getType());
        entity.setPassword(MD5Utils.encrypt(userInfo.getPassword()));
        entity.setDueDate(DateUtils.getExpirationDate(userInfo.getValidDays()));
        entity.setCreateBy(LoginUserUtils.getLoginUserDetail().getUsername());
        entity.setName(username);
        try {
            Map<String, String> keyPairs = RSAUtils.generateRSAKeyPairs();
            String publicKey = keyPairs.get(RSAUtils.PUBLIC_KEY);
            String privateKey = keyPairs.get(RSAUtils.PRIVATE_KEY);
            String secretKey = RandomStringUtils.randomAlphanumeric(8);
            Integer encryptVersion = AESUtils.getCurrentVersion(null);
            entity.setEncryptVersion(encryptVersion);
            entity.setPublicKey(AESUtils.encryptToString(publicKey.getBytes(StandardCharsets.UTF_8), encryptVersion));
            entity.setPrivateKey(AESUtils.encryptToString(privateKey.getBytes(StandardCharsets.UTF_8), encryptVersion));
            entity.setSecretKey(AESUtils.encryptToString(secretKey.getBytes(StandardCharsets.UTF_8), encryptVersion));
        } catch (Exception e) {
            String errMsg = String.format("generate rsa key error: %s", e.getMessage());
            log.error(errMsg, e);
            throw new BusinessException(errMsg);
        }

        entity.setCreateTime(new Date());
        Preconditions.checkTrue(userMapper.insert(entity) > 0, "Create user failed");

        log.debug("success to create user info={}", userInfo);
        return true;
    }

    @Override
    public int update(UserInfo userInfo, String currentUser) {
        Preconditions.checkNotNull(userInfo, "user info should not be null");
        Preconditions.checkNotNull(userInfo.getId(), "user id should not be null");

        // Whether the current user is an administrator
        UserEntity userEntity = getByUsername(currentUser);
        boolean isAdmin = userEntity.getAccountType().equals(UserTypeEnum.ADMIN.getCode());
        Preconditions.checkTrue(isAdmin || Objects.equals(userInfo.getUsername(), currentUser),
                "Current user is not a manager and does not have permission to update users");
        Preconditions.checkFalse(isAdmin && userInfo.getType().equals(UserTypeEnum.OPERATOR.getCode())
                        && Objects.equals(userInfo.getUsername(), currentUser),
                "Administrators cannot be set himself as ordinary users");

        UserEntity entity = userMapper.selectByPrimaryKey(userInfo.getId());
        Preconditions.checkNotNull(entity, "User not exists with id " + userInfo.getId());
        UserEntity userExist = getByUsername(userInfo.getUsername());
        Preconditions.checkTrue(Objects.equals(userExist.getName(), entity.getName())
                && !Objects.equals(userExist.getId(), entity.getId()),
                "username [" + userInfo.getUsername() + "] already exists");

        if (!isAdmin) {
            String oldPassword = userInfo.getPassword();
            String oldPasswordMd = MD5Utils.encrypt(oldPassword);
            Preconditions.checkTrue(oldPasswordMd.equals(entity.getPassword()), "Old password is wrong");
            Integer validDays = DateUtils.getValidDays(entity.getCreateTime(), entity.getDueDate());
            Preconditions.checkFalse((userInfo.getValidDays() > validDays),
                    "Operator is not allowed to add valid days as ordinary users");
            Preconditions.checkTrue(Objects.equals(entity.getAccountType(), userInfo.getType()),
                    "Operator is not allowed to update account type as ordinary users");
        }
        // update password
        String newPasswordMd5 = MD5Utils.encrypt(userInfo.getNewPassword());
        entity.setPassword(newPasswordMd5);
        entity.setDueDate(DateUtils.getExpirationDate(userInfo.getValidDays()));
        entity.setAccountType(userInfo.getType());
        entity.setName(userInfo.getUsername());

        log.debug("success to update user info={}", userInfo);
        return userMapper.updateByPrimaryKeySelective(entity);
    }

    @Override
    public Boolean delete(Integer userId, String currentUser) {
        Preconditions.checkNotNull(userId, "User id should not be empty");

        // Whether the current user is an administrator
        UserEntity curUser = getByUsername(currentUser);
        UserEntity entity = userMapper.selectByPrimaryKey(userId);
        Preconditions.checkTrue(curUser.getAccountType().equals(UserTypeEnum.ADMIN.getCode()),
                "Current user is not a manager and does not have permission to delete users");
        Preconditions.checkTrue(!Objects.equals(entity.getName(), currentUser),
                "Current user does not have permission to delete himself");
        userMapper.deleteByPrimaryKey(userId);
        log.debug("success to delete user by id={}, current user={}", userId, currentUser);
        return true;
    }

    @Override
    public PageInfo<UserDetailListVO> list(UserDetailPageRequest request) {
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        UserEntityExample example = new UserEntityExample();
        Criteria criteria = example.createCriteria();
        if (request.getUsername() != null) {
            criteria.andNameLike(request.getUsername() + "%");
        }

        Page<UserEntity> entityPage = (Page<UserEntity>) userMapper.selectByExample(example);
        List<UserDetailListVO> detailList = CommonBeanUtils.copyListProperties(entityPage, UserDetailListVO::new);
        // Check whether the user account has expired
        detailList.forEach(
                entity -> entity.setStatus(entity.getDueDate().after(new Date()) ? "valid" : "invalid"));
        PageInfo<UserDetailListVO> page = new PageInfo<>(detailList);
        page.setTotal(entityPage.getTotal());

        log.debug("success to list all user, result size={}", page.getTotal());
        return page;
    }

}
