/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// package util defines the constants and helper functions.
package util

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// InValidValue of TubeMQ config.
var InvalidValue = int64(-2)

// GetLocalHost returns the local host name.
func GetLocalHost() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return ""
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String()
		}
	}
	return ""
}

// GenBrokerAuthenticateToken generates the broker authenticate token.
func GenBrokerAuthenticateToken(username string, password string) string {
	return ""
}

// GenMasterAuthenticateToken generates the master authenticate token.
func GenMasterAuthenticateToken(username string, password string) string {
	return ""
}

// ParseConfirmContext parses the confirmcontext to partition key and bookedTime.
func ParseConfirmContext(confirmContext string) (string, int64, error) {
	res := strings.Split(confirmContext, "@")
	if len(res) == 0 {
		return "", 0, fmt.Errorf("illegal confirmContext content: unregular value format")
	}
	partitionKey := res[0]
	bookedTime, err := strconv.ParseInt(res[1], 10, 64)
	if err != nil {
		return "", 0, err
	}
	return partitionKey, bookedTime, nil
}

func SplitToMap(source string, step1 string, step2 string) map[string]string {
	pos1 := 0
	pos2 := strings.Index(source, step1)
	pos3 := 0
	m := make(map[string]string)
	for pos2 != -1 {
		itemStr := strings.TrimSpace(source[pos1:pos2-pos1])
		if len(itemStr) == 0 {
			continue
		}
		pos1 = pos2 + len(step1)
		pos2 = strings.Index(source[pos1:], step1)
		pos3 = strings.Index(itemStr, step2)
		if pos3 == -1 {
			continue
		}
		key := strings.TrimSpace(itemStr[:pos3])
		val := strings.TrimSpace(itemStr[pos3+len(step2):])
		if len(key) == 0 {
			continue
		}
		m[key] = val
	}
	if pos1 != len(source) {
		itemStr := strings.TrimSpace(source[pos1:])
		pos3 = strings.Index(itemStr, step2)
		if pos3 != - 1{
			key := strings.TrimSpace(itemStr[:pos3])
			val := strings.TrimSpace(itemStr[pos3+len(step2):])
			if len(key) > 0 {
				m[key] = val
			}
		}
	}
	return m
}