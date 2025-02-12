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

package org.apache.inlong.sdk.dataproxy.network;

import org.apache.inlong.sdk.dataproxy.common.ErrorCode;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.common.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.common.SdkConsts;
import org.apache.inlong.sdk.dataproxy.utils.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;

/**
 * Package Cache Quota class
 *
 * Used to manage the total number and byte size of ongoing requests.
 */
public class PkgCacheQuota {
    private static final Logger logger = LoggerFactory.getLogger(PkgCacheQuota.class);
    private static final Tuple2<Integer, Integer> DISABLE_RET =
            new Tuple2<>(SdkConsts.UNDEFINED_VALUE, SdkConsts.UNDEFINED_VALUE);
    // whether factory level quota
    private final boolean factoryLevel;
    // pkg count permits
    private final int pkgCntPermits;
    // pkg size permits
    private final int pkgSizeKbPermits;
    // reserved size per package
    private final int paddingSizePerPkg;
    // whether disable package inflight quota function
    private final boolean disabled;
    // package count quota
    private final Semaphore pkgCntQuota;
    // package size quota in KB
    private final Semaphore pkgSizeKbQuota;

    public PkgCacheQuota(boolean factoryLevel, String parentId,
            int pkgCntPermits, int pkgSizeKbPermits, int paddingSizePerPkg) {
        this.factoryLevel = factoryLevel;
        this.pkgCntPermits = pkgCntPermits;
        this.pkgSizeKbPermits = pkgSizeKbPermits;
        this.paddingSizePerPkg = paddingSizePerPkg;
        if (pkgCntPermits > 0) {
            this.pkgCntQuota = new Semaphore(pkgCntPermits);
        } else {
            this.pkgCntQuota = null;
        }
        if (pkgSizeKbPermits > 0) {
            this.pkgSizeKbQuota = new Semaphore(pkgSizeKbPermits);
        } else {
            this.pkgSizeKbQuota = null;
        }
        this.disabled = (this.pkgCntQuota == null
                && this.pkgSizeKbQuota == null);
        logger.info("PkgCacheQuota({}) created, factoryLevel={},pkgCnt={},pkgSizeKb={},padSize={}",
                factoryLevel, parentId, pkgCntPermits, pkgSizeKbPermits, paddingSizePerPkg);
    }

    public Tuple2<Integer, Integer> getPkgCacheAvailQuota() {
        if (this.disabled) {
            return DISABLE_RET;
        }
        int cntAvailable = SdkConsts.UNDEFINED_VALUE;
        if (this.pkgCntQuota != null) {
            cntAvailable = this.pkgCntQuota.availablePermits();
        }
        int sizeAvailable = SdkConsts.UNDEFINED_VALUE;
        if (this.pkgSizeKbQuota != null) {
            sizeAvailable = this.pkgSizeKbQuota.availablePermits();
        }
        return new Tuple2<>(cntAvailable, sizeAvailable);
    }

    public boolean tryAcquire(int sizeInByte, ProcessResult procResult) {
        if (this.disabled) {
            return procResult.setSuccess();
        }
        if (this.pkgCntQuota == null) {
            if (this.pkgSizeKbQuota.tryAcquire(
                    getSizeKbPermitsByBytes(sizeInByte))) {
                return procResult.setSuccess();
            }
            return procResult.setFailResult(factoryLevel
                    ? ErrorCode.INF_REQ_SIZE_REACH_FACTORY_LIMIT
                    : ErrorCode.INF_REQ_SIZE_REACH_SDK_LIMIT);
        } else {
            if (this.pkgCntQuota.tryAcquire(1)) {
                if (this.pkgSizeKbQuota == null) {
                    return procResult.setSuccess();
                } else {
                    if (this.pkgSizeKbQuota.tryAcquire(
                            getSizeKbPermitsByBytes(sizeInByte))) {
                        return procResult.setSuccess();
                    } else {
                        this.pkgCntQuota.release();
                        return procResult.setFailResult(factoryLevel
                                ? ErrorCode.INF_REQ_SIZE_REACH_FACTORY_LIMIT
                                : ErrorCode.INF_REQ_SIZE_REACH_SDK_LIMIT);
                    }
                }
            } else {
                return procResult.setFailResult(factoryLevel
                        ? ErrorCode.INF_REQ_COUNT_REACH_FACTORY_LIMIT
                        : ErrorCode.INF_REQ_COUNT_REACH_SDK_LIMIT);
            }
        }
    }

    public void release(int sizeInByte) {
        if (this.disabled) {
            return;
        }
        if (this.pkgCntQuota == null) {
            this.pkgSizeKbQuota.release(
                    getSizeKbPermitsByBytes(sizeInByte));
        } else {
            if (this.pkgSizeKbQuota != null) {
                this.pkgSizeKbQuota.release(
                        getSizeKbPermitsByBytes(sizeInByte));
            }
            this.pkgCntQuota.release();
        }
    }

    public boolean isFactoryLevel() {
        return factoryLevel;
    }

    public int getPkgCntPermits() {
        return pkgCntPermits;
    }

    public int getPkgSizeKbPermits() {
        return pkgSizeKbPermits;
    }

    public int getPaddingSizePerPkg() {
        return paddingSizePerPkg;
    }

    public boolean isDisabled() {
        return disabled;
    }

    private int getSizeKbPermitsByBytes(int sizeInByte) {
        int tmpValue = sizeInByte + this.paddingSizePerPkg;
        if (tmpValue % SdkConsts.UNIT_KB_SIZE == 0) {
            return (tmpValue / SdkConsts.UNIT_KB_SIZE);
        } else {
            return (tmpValue / SdkConsts.UNIT_KB_SIZE) + 1;
        }
    }
}
