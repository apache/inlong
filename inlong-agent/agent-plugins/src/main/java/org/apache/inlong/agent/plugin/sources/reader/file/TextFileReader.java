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

package org.apache.inlong.agent.plugin.sources.reader.file;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.inlong.agent.constant.JobConstants.JOB_FILE_LINE_END_PATTERN;
import static org.apache.inlong.agent.constant.JobConstants.JOB_FILE_MONITOR_DEFAULT_STATUS;
import static org.apache.inlong.agent.constant.JobConstants.JOB_FILE_MONITOR_STATUS;

/**
 * Text file reader
 */
public final class TextFileReader extends AbstractFileReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(TextFileReader.class);

    private static final int LINE_SEPARATOR_SIZE = System.lineSeparator().getBytes(StandardCharsets.UTF_8).length;

    private static final int BATCH_READ_SIZE = 10000;

    private final StringBuffer sb = new StringBuffer();

    private int bytePosition = 0;

    public TextFileReader(FileReaderOperator fileReaderOperator) {
        super.fileReaderOperator = fileReaderOperator;
        if (fileReaderOperator.jobConf.get(JOB_FILE_MONITOR_STATUS, JOB_FILE_MONITOR_DEFAULT_STATUS)
                .equals(JOB_FILE_MONITOR_DEFAULT_STATUS)) {
            MonitorTextFile.getInstance().monitor(fileReaderOperator, this);
        }
    }

    public void getData() throws IOException {
        // todo: TaskPositionManager stored position should be changed to byte position.Now it store msg sent, so here
        //  every line (include empty line) should be sent, otherwise the read position will be offset when
        //  restarting and recovering. In the same time, Regex end line spiltted line also has this problem, because
        //  recovering is based on line position.
        List<String> lines = bytePosition == 0 ? readFromLine(fileReaderOperator.position) : readFromPos(bytePosition);
        LOGGER.info("path is {}, line is {}, position is {}, data reads size {}",
                fileReaderOperator.file.getName(), fileReaderOperator.position, bytePosition, lines.size());
        List<String> resultLines = lines;
        //TODO line regular expression matching
        if (fileReaderOperator.jobConf.hasKey(JOB_FILE_LINE_END_PATTERN)) {
            Pattern pattern = Pattern.compile(fileReaderOperator.jobConf.get(JOB_FILE_LINE_END_PATTERN));
            resultLines = lines.stream().flatMap(line -> {
                sb.append(line + System.lineSeparator());
                String data = sb.toString();
                Matcher matcher = pattern.matcher(data);
                List<String> tmpResultLines = new ArrayList<>();
                int beginPos = 0;
                while (matcher.find()) {
                    String endLineStr = matcher.group();
                    int endPos = data.indexOf(endLineStr, beginPos);
                    tmpResultLines.add(data.substring(beginPos, endPos));
                    beginPos = endPos + endLineStr.length();
                }
                String lastWord = data.substring(beginPos);
                sb.setLength(0);
                sb.append(lastWord);
                return tmpResultLines.stream();
            }).collect(Collectors.toList());
        }

        bytePosition += lines.stream()
                .mapToInt(line -> line.getBytes(StandardCharsets.UTF_8).length + LINE_SEPARATOR_SIZE)
                .sum();
        fileReaderOperator.stream = resultLines.stream();
        fileReaderOperator.position = fileReaderOperator.position + lines.size();
    }

    private List<String> readFromLine(int lineNum) throws IOException {
        String line = null;
        List<String> lines = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(fileReaderOperator.file));
        int count = 0;
        while ((line = reader.readLine()) != null) {
            if (++count > fileReaderOperator.position) {
                lines.add(line);
            }
            if (lines.size() >= BATCH_READ_SIZE) {
                break;
            }
        }
        return lines;
    }

    private List<String> readFromPos(int pos) throws IOException {
        String line = null;
        List<String> lines = new ArrayList<>();
        DataInput input = new RandomAccessFile(fileReaderOperator.file, "r");
        input.skipBytes(pos);
        while ((line = input.readLine()) != null) {
            lines.add(line);
            if (lines.size() >= BATCH_READ_SIZE) {
                break;
            }
        }
        return lines;
    }
}
