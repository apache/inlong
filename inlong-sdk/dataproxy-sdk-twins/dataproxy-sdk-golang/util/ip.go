//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// IPtoUInt converts an IP to an uint64 integer
func IPtoUInt(ip string) uint32 {
	ips := net.ParseIP(ip)

	if len(ips) == 16 {
		return binary.BigEndian.Uint32(ips[12:16])
	} else if len(ips) == 4 {
		return binary.BigEndian.Uint32(ips)
	}
	return 0
}

// IsPrivateIP checks if an IP is a private one, according to some reserved inner IP rules, may be not suitable for all
func IsPrivateIP(ip string) bool {
	ipByte := strings.Split(ip, ".")
	if len(ipByte) != 4 {
		return false
	}

	first, err := strconv.Atoi(ipByte[0])
	if err != nil {
		return false
	}

	second, err := strconv.Atoi(ipByte[1])
	if err != nil {
		return false
	}

	third, err := strconv.Atoi(ipByte[2])
	if err != nil {
		return false
	}

	fourth, err := strconv.Atoi(ipByte[3])
	if err != nil {
		return false
	}

	if first == 11 || first == 10 || first == 9 || first == 30 || first == 21 ||
		(first == 100 && second >= 64 && second <= 127) ||
		(first == 172 && second >= 16 && second <= 31) ||
		(first == 192 && second == 168) ||
		(first == 172 && second == 32 && (third == 0 || third == 1) && fourth >= 1 && fourth <= 128) {
		return true
	}

	return false
}

// GetIPv4List obtain all valid local addresses
func GetIPv4List() ([]string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	var ipv4List []string
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}

			ip := ipNet.IP
			if ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}

			if ipv4 := ip.To4(); ipv4 != nil {
				ipv4List = append(ipv4List, ipv4.String())
			}
		}
	}
	return ipv4List, nil
}

// GetPrivateIPList gets all the private IPs of the current host
func GetPrivateIPList() ([]string, error) {
	ips, err := GetIPv4List()
	if err != nil {
		return nil, err
	}

	var privateIPs []string
	for _, ip := range ips {
		if IsPrivateIP(ip) {
			privateIPs = append(privateIPs, ip)
		}
	}

	return privateIPs, nil
}

// GetFirstPrivateIP gets the first private IP of the current host
func GetFirstPrivateIP() (string, error) {
	ips, err := GetPrivateIPList()
	if err != nil {
		return "", err
	}
	if len(ips) == 0 {
		return "", fmt.Errorf("no private ip")
	}

	return ips[0], nil
}

// GetFirstIP gets the first IP of the current host
func GetFirstIP() (string, error) {
	ips, err := GetIPv4List()
	if err != nil {
		return "", err
	}

	if len(ips) > 0 {
		return ips[0], nil
	}

	return "", fmt.Errorf("no ip")
}

// GetOneIP obtain a valid ip address of the current host, with private ip preferred
func GetOneIP() (string, error) {
	ips, err := GetIPv4List()
	if err != nil {
		return "", fmt.Errorf("failed to obtain ip. %w", err)
	}

	if len(ips) == 0 {
		return "", fmt.Errorf("no ip")
	}

	for _, ip := range ips {
		if IsPrivateIP(ip) {
			return ip, nil
		}
	}

	return ips[0], nil
}
