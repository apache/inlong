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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
@Service
public class UserServiceImpl implements UserService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserServiceImpl.class);

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
        Preconditions.checkTrue(Objects.equals(UserTypeEnum.ADMIN.getCode(), curUser.getAccountType())
                        || Objects.equals(entity.getName(), currentUser),
                "Current user does not have permission to get other users' info");

        UserInfo result = new UserInfo();
        result.setId(entity.getId());
        result.setUsername(entity.getName());
        result.setValidDays(DateUtils.getValidDays(entity.getCreateTime(), entity.getDueDate()));
        result.setType(entity.getAccountType());
        result.setVersion(entity.getVersion());

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
                LOGGER.error(errMsg, e);
                throw new BusinessException(errMsg);
            }
        }

        LOGGER.debug("success to get user info by id={}", userId);
        return result;
    }

    @Override
    public boolean create(UserInfo userInfo) {
        String username = userInfo.getUsername();
        UserEntity userExists = getByUsername(username);
        String password = userInfo.getPassword();
        Preconditions.checkNull(userExists, "username [" + username + "] already exists");
        Preconditions.checkTrue(StringUtils.isNotBlank(password), "password cannot be blank");

        UserEntity entity = new UserEntity();
        entity.setAccountType(userInfo.getType());
        entity.setPassword(MD5Utils.encrypt(password));
        entity.setDueDate(DateUtils.getExpirationDate(userInfo.getValidDays()));
        String currentUser = LoginUserUtils.getLoginUserDetail().getUsername();
        entity.setCreateBy(currentUser);
        entity.setUpdateBy(currentUser);
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
            LOGGER.error(errMsg, e);
            throw new BusinessException(errMsg);
        }

        Preconditions.checkTrue(userMapper.insert(entity) > 0, "Create user failed");

        LOGGER.debug("success to create user info={}", userInfo);
        return true;
    }

    @Override
    public int update(UserInfo updateUser, String currentUser) {
        LOGGER.debug("begin to update user info={} by {}", updateUser, currentUser);
        Preconditions.checkNotNull(updateUser, "Userinfo cannot be null");
        Preconditions.checkNotNull(updateUser.getId(), "User id cannot be null");

        // Whether the current user is a manager
        UserEntity currentUserEntity = getByUsername(currentUser);
        boolean isAdmin = Objects.equals(UserTypeEnum.ADMIN.getCode(), currentUserEntity.getAccountType());
        Preconditions.checkTrue(isAdmin || Objects.equals(updateUser.getUsername(), currentUser),
                "You are not a manager and do not have permission to update other users");

        // manager cannot set himself as an ordinary
        boolean managerToOrdinary = isAdmin
                && Objects.equals(UserTypeEnum.OPERATOR.getCode(), updateUser.getType())
                && Objects.equals(currentUser, updateUser.getUsername());
        Preconditions.checkFalse(managerToOrdinary, "You are a manager and you cannot change to an ordinary user");

        // target username must not exist
        UserEntity updateUserEntity = userMapper.selectByPrimaryKey(updateUser.getId());
        Preconditions.checkNotNull(updateUserEntity, "User not exists with id=" + updateUser.getId());
        UserEntity targetUserEntity = getByUsername(updateUser.getUsername());
        Preconditions.checkTrue(Objects.equals(targetUserEntity.getName(), updateUserEntity.getName())
                        && !Objects.equals(targetUserEntity.getId(), updateUserEntity.getId()),
                "Username [" + updateUser.getUsername() + "] already exists");
        String errMsg = String.format("user has already updated with username=%s, curVersion=%s",
                updateUser.getUsername(), updateUser.getVersion());
        if (!Objects.equals(updateUserEntity.getVersion(), updateUser.getVersion())) {
            LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }

        // if the current user is not a manager, needs to check the password before updating user info
        if (!isAdmin) {
            String oldPassword = updateUser.getPassword();
            String oldPasswordMd = MD5Utils.encrypt(oldPassword);
            Preconditions.checkTrue(oldPasswordMd.equals(updateUserEntity.getPassword()), "Old password is wrong");
            Integer validDays = DateUtils.getValidDays(updateUserEntity.getCreateTime(), updateUserEntity.getDueDate());
            Preconditions.checkTrue((updateUser.getValidDays() <= validDays),
                    "Ordinary users are not allowed to add valid days");
            Preconditions.checkTrue(Objects.equals(updateUserEntity.getAccountType(), updateUser.getType()),
                    "Ordinary users are not allowed to update account type");
        }

        // update password
        if (!StringUtils.isBlank(updateUser.getNewPassword())) {
            String newPasswordMd5 = MD5Utils.encrypt(updateUser.getNewPassword());
            updateUserEntity.setPassword(newPasswordMd5);
        }
        updateUserEntity.setDueDate(DateUtils.getExpirationDate(updateUser.getValidDays()));
        updateUserEntity.setAccountType(updateUser.getType());
        updateUserEntity.setName(updateUser.getUsername());

        int rowCount = userMapper.updateByPrimaryKeySelective(updateUserEntity);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        LOGGER.debug("success to update user info={} by {}", updateUser, currentUser);
        return updateUserEntity.getId();
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

        LOGGER.debug("success to delete user by id={}, current user={}", userId, currentUser);
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
        detailList.forEach(entity -> entity.setStatus(entity.getDueDate().after(new Date()) ? "valid" : "invalid"));
        PageInfo<UserDetailListVO> page = new PageInfo<>(detailList);
        page.setTotal(entityPage.getTotal());

        LOGGER.debug("success to list users, result size={}", page.getTotal());
        return page;
    }

}
