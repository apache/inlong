/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.corebase.metric;

import java.util.Map;

/**
 *
 * Time consumption statistics category, currently includes the max, min, and
 *   17 histogram data statistical intervals(multiplied by the exponential of 2).
 *   The time consumption can be increased or reduced according to the coefficient.
 *   According to the metric data output by the Map, the key will be composed with Name
 *   as the prefix for content association of output indicators under this category
 */
public class TimeDltMetricItem {
    private final String name;
    private final String itemPrefix;
    private final boolean enableHistogram;
    private final ValueAdjustType adjustType;
    private final int multiple;

    // statistic count
    private final AbsMetricItem totalCount =
            new CountMetricItem("count");
    private final String totalCntJmxKey;
    // boundary values
    protected final AbsMetricItem procTimeDltLst =
            new GaugeNormMetricItem("dlt_ms_lst");
    private final String dltLastJmxKey;
    private final AbsMetricItem procTimeDltMin =
            new GaugeMinMetricItem("dlt_ms_min");
    private final String dltMinJmxKey;
    private final AbsMetricItem procTimeDltMax =
            new GaugeMaxMetricItem("dlt_ms_max");
    private final String dltMaxJmxKey;
    // time dlt from 000 ~ 1024
    private final AbsMetricItem procTimeDlt000000T000004 =
            new CountMetricItem("dlt_ms_000000t000004");
    private final AbsMetricItem procTimeDlt000004T000008 =
            new CountMetricItem("dlt_ms_000004t000008");
    private final AbsMetricItem procTimeDlt000008T000016 =
            new CountMetricItem("dlt_ms_000008t000016");
    private final AbsMetricItem procTimeDlt000016T000032 =
            new CountMetricItem("dlt_ms_000016t000032");
    private final AbsMetricItem procTimeDlt000032T000064 =
            new CountMetricItem("dlt_ms_000032t000064");
    private final AbsMetricItem procTimeDlt000064T000128 =
            new CountMetricItem("dlt_ms_000064t000128");
    private final AbsMetricItem procTimeDlt000128T000256 =
            new CountMetricItem("dlt_ms_000128t000256");
    private final AbsMetricItem procTimeDlt000256T000512 =
            new CountMetricItem("dlt_ms_000256t000512");
    private final AbsMetricItem procTimeDlt000512T001024 =
            new CountMetricItem("dlt_ms_000512t001024");
    // time dlt from 1024 ~ max
    private final AbsMetricItem procTimeDlt001024T002048 =
            new CountMetricItem("dlt_ms_001024t002048");
    private final AbsMetricItem procTimeDlt002048T004096 =
            new CountMetricItem("dlt_ms_002048t004096");
    private final AbsMetricItem procTimeDlt004096T008192 =
            new CountMetricItem("dlt_ms_004096t008192");
    private final AbsMetricItem procTimeDlt008192T016384 =
            new CountMetricItem("dlt_ms_008192t016384");
    private final AbsMetricItem procTimeDlt016384T032768 =
            new CountMetricItem("dlt_ms_016384t032768");
    private final AbsMetricItem procTimeDlt032768T065535 =
            new CountMetricItem("dlt_ms_032768t065535");
    private final AbsMetricItem procTimeDlt065535T131072 =
            new CountMetricItem("dlt_ms_065535t131072");
    private final AbsMetricItem procTimeDlt131072Tffffff =
            new CountMetricItem("dlt_ms_131072tffffff");

    public TimeDltMetricItem(String metricName) {
        this(metricName, false, ValueAdjustType.KEEPSAME, 1);
    }

    public TimeDltMetricItem(String metricName, boolean enableHistogram) {
        this(metricName, enableHistogram, ValueAdjustType.KEEPSAME, 1);
    }

    public TimeDltMetricItem(String metricName, boolean enableHistogram,
                             ValueAdjustType adjustType, int multiple) {
        if (metricName.endsWith("_")) {
            this.name = metricName.substring(0, metricName.length() - 1);
            this.itemPrefix = metricName;
        } else {
            this.name = metricName;
            this.itemPrefix = metricName + "_";
        }
        this.enableHistogram = enableHistogram;
        this.adjustType = adjustType;
        if (adjustType != ValueAdjustType.KEEPSAME && multiple == 0) {
            this.multiple = 1;
        } else {
            this.multiple = multiple;
        }
        StringBuilder strBuff = new StringBuilder(512);
        this.totalCntJmxKey = strBuff.append(itemPrefix)
                .append(totalCount.getName()).toString();
        strBuff.delete(0, strBuff.length());
        this.dltLastJmxKey = strBuff.append(itemPrefix)
                .append(procTimeDltLst.getName()).toString();
        strBuff.delete(0, strBuff.length());
        this.dltMinJmxKey = strBuff.append(itemPrefix)
                .append(procTimeDltMin.getName()).toString();
        strBuff.delete(0, strBuff.length());
        this.dltMaxJmxKey = strBuff.append(itemPrefix)
                .append(procTimeDltMax.getName()).toString();
        strBuff.delete(0, strBuff.length());
    }

    public String getTotalCntKey() {
        return this.totalCntJmxKey;
    }

    public String getDltLastJmxKey() {
        return this.dltLastJmxKey;
    }

    public String getDltMinJmxKey() {
        return dltMinJmxKey;
    }

    public String getDltMaxJmxKey() {
        return dltMaxJmxKey;
    }

    public void getProcTimeDltDuration(Map<String, Long> metricValues,
                                       boolean resetValue) {
        // total measure count
        metricValues.put(totalCntJmxKey, totalCount.getValue(resetValue));
        // the latest value
        metricValues.put(dltLastJmxKey, procTimeDltLst.getValue(resetValue));
        // min and max value
        metricValues.put(dltMinJmxKey, procTimeDltMin.getValue(resetValue));
        metricValues.put(dltMaxJmxKey, procTimeDltMax.getValue(resetValue));
    }

    public void updProcTimeDlt(long dltTime) {
        // update boundary values
        totalCount.incrementAndGet();
        procTimeDltLst.update(dltTime);
        procTimeDltMin.update(dltTime);
        procTimeDltMax.update(dltTime);
        if (enableHistogram) {
            // adjust value size
            if (adjustType != ValueAdjustType.KEEPSAME) {
                if (adjustType == ValueAdjustType.ZOOMIN) {
                    dltTime *= multiple;
                } else {
                    dltTime /= multiple;
                }
            }
            // statistic histogram
            if (dltTime < 4) {
                procTimeDlt000000T000004.incrementAndGet();
            } else if (dltTime < 8) {
                procTimeDlt000004T000008.incrementAndGet();
            } else if (dltTime < 16) {
                procTimeDlt000008T000016.incrementAndGet();
            } else if (dltTime < 32) {
                procTimeDlt000016T000032.incrementAndGet();
            } else if (dltTime < 64) {
                procTimeDlt000032T000064.incrementAndGet();
            } else if (dltTime < 128) {
                procTimeDlt000064T000128.incrementAndGet();
            } else if (dltTime < 256) {
                procTimeDlt000128T000256.incrementAndGet();
            } else if (dltTime < 512) {
                procTimeDlt000256T000512.incrementAndGet();
            } else if (dltTime < 1024) {
                procTimeDlt000512T001024.incrementAndGet();
            } else if (dltTime < 2048) {
                procTimeDlt001024T002048.incrementAndGet();
            } else if (dltTime < 4096) {
                procTimeDlt002048T004096.incrementAndGet();
            } else if (dltTime < 8192) {
                procTimeDlt004096T008192.incrementAndGet();
            } else if (dltTime < 16384) {
                procTimeDlt008192T016384.incrementAndGet();
            } else if (dltTime < 32768) {
                procTimeDlt016384T032768.incrementAndGet();
            } else if (dltTime < 65535) {
                procTimeDlt032768T065535.incrementAndGet();
            } else if (dltTime < 131072) {
                procTimeDlt065535T131072.incrementAndGet();
            } else {
                procTimeDlt131072Tffffff.incrementAndGet();
            }
        }
    }

    public void getMapMetrics(Map<String, Long> metricValues, boolean resetValue) {
        // total measure count
        metricValues.put(totalCntJmxKey, totalCount.getValue(resetValue));
        // the latest value
        metricValues.put(dltLastJmxKey, procTimeDltLst.getValue(resetValue));
        // min and max value
        metricValues.put(dltMinJmxKey, procTimeDltMin.getValue(resetValue));
        metricValues.put(dltMaxJmxKey, procTimeDltMax.getValue(resetValue));
        if (!enableHistogram) {
            return;
        }
        StringBuilder strBuff = new StringBuilder(512);
        metricValues.put(strBuff.append(itemPrefix)
                .append(adjustType.getName()).toString(), (long) multiple);
        strBuff.delete(0, strBuff.length());
        // 00000 ~ 00128
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt000000T000004.getName()).toString(),
                procTimeDlt000000T000004.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt000004T000008.getName()).toString(),
                procTimeDlt000004T000008.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt000008T000016.getName()).toString(),
                procTimeDlt000008T000016.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt000016T000032.getName()).toString(),
                procTimeDlt000016T000032.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt000032T000064.getName()).toString(),
                procTimeDlt000032T000064.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt000064T000128.getName()).toString(),
                procTimeDlt000064T000128.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        // 00128 ~ 04096
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt000128T000256.getName()).toString(),
                procTimeDlt000128T000256.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt000256T000512.getName()).toString(),
                procTimeDlt000256T000512.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt000512T001024.getName()).toString(),
                procTimeDlt000512T001024.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt001024T002048.getName()).toString(),
                procTimeDlt001024T002048.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt002048T004096.getName()).toString(),
                procTimeDlt002048T004096.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        // 04096 ~ max
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt004096T008192.getName()).toString(),
                procTimeDlt004096T008192.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt008192T016384.getName()).toString(),
                procTimeDlt008192T016384.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt016384T032768.getName()).toString(),
                procTimeDlt016384T032768.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt032768T065535.getName()).toString(),
                procTimeDlt032768T065535.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt065535T131072.getName()).toString(),
                procTimeDlt065535T131072.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
        metricValues.put(strBuff.append(itemPrefix)
                        .append(procTimeDlt131072Tffffff.getName()).toString(),
                procTimeDlt131072Tffffff.getValue(resetValue));
        strBuff.delete(0, strBuff.length());
    }

    public void getStrMetrics(StringBuilder strBuff, boolean resetValue) {
        strBuff.append("\"").append(name).append("\":{\"")
                .append(totalCount.getName()).append("\":")
                .append(totalCount.getValue(resetValue)).append(",\"")
                .append(procTimeDltLst.getName()).append("\":")
                .append(procTimeDltLst.getValue(resetValue)).append(",\"")
                .append(procTimeDltMin.getName()).append("\":")
                .append(procTimeDltMin.getValue(resetValue)).append(",\"")
                .append(procTimeDltMax.getName()).append("\":")
                .append(procTimeDltMax.getValue(resetValue));
        if (enableHistogram) {
            strBuff.append(",\"dlt_histogram\":{\"adjustType\":\"").append(adjustType.getName())
                    .append("\",\"multiple\":").append(multiple).append(",\"")
                    .append(procTimeDlt000000T000004.getName()).append("\":")
                    .append(procTimeDlt000000T000004.getValue(resetValue))
                    .append(",\"").append(procTimeDlt000004T000008.getName()).append("\":")
                    .append(procTimeDlt000004T000008.getValue(resetValue))
                    .append(",\"").append(procTimeDlt000008T000016.getName()).append("\":")
                    .append(procTimeDlt000008T000016.getValue(resetValue))
                    .append(",\"").append(procTimeDlt000016T000032.getName()).append("\":")
                    .append(procTimeDlt000016T000032.getValue(resetValue))
                    .append(",\"").append(procTimeDlt000032T000064.getName()).append("\":")
                    .append(procTimeDlt000032T000064.getValue(resetValue))
                    .append(",\"").append(procTimeDlt000064T000128.getName()).append("\":")
                    .append(procTimeDlt000064T000128.getValue(resetValue))
                    .append(",\"").append(procTimeDlt000128T000256.getName()).append("\":")
                    .append(procTimeDlt000128T000256.getValue(resetValue))
                    .append(",\"").append(procTimeDlt000256T000512.getName()).append("\":")
                    .append(procTimeDlt000256T000512.getValue(resetValue))
                    .append(",\"").append(procTimeDlt000512T001024.getName()).append("\":")
                    .append(procTimeDlt000512T001024.getValue(resetValue))
                    .append(",\"").append(procTimeDlt001024T002048.getName()).append("\":")
                    .append(procTimeDlt001024T002048.getValue(resetValue))
                    .append(",\"").append(procTimeDlt002048T004096.getName()).append("\":")
                    .append(procTimeDlt002048T004096.getValue(resetValue))
                    .append(",\"").append(procTimeDlt004096T008192.getName()).append("\":")
                    .append(procTimeDlt004096T008192.getValue(resetValue))
                    .append(",\"").append(procTimeDlt008192T016384.getName()).append("\":")
                    .append(procTimeDlt008192T016384.getValue(resetValue))
                    .append(",\"").append(procTimeDlt016384T032768.getName()).append("\":")
                    .append(procTimeDlt016384T032768.getValue(resetValue))
                    .append(",\"").append(procTimeDlt032768T065535.getName()).append("\":")
                    .append(procTimeDlt032768T065535.getValue(resetValue))
                    .append(",\"").append(procTimeDlt065535T131072.getName()).append("\":")
                    .append(procTimeDlt065535T131072.getValue(resetValue))
                    .append(",\"").append(procTimeDlt131072Tffffff.getName()).append("\":")
                    .append(procTimeDlt131072Tffffff.getValue(resetValue))
                    .append("}}");
        } else {
            strBuff.append("}");
        }
    }
}
