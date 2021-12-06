package org.apache.inlong.audit.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class Config {
    private String localIP = "";
    private String dockerId = "";

    public void init() {
        initIP();
        initDockerId();
    }

    public String getLocalIP() {
        return localIP;
    }

    public String getDockerId() {
        return dockerId;
    }

    private void initIP() {
        try {
            for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements(); ) {
                NetworkInterface intf = en.nextElement();
                String name = intf.getName();
                if (!name.contains("docker") && !name.contains("lo")) {
                    for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements(); ) {
                        InetAddress inetAddress = enumIpAddr.nextElement();
                        if (!inetAddress.isLoopbackAddress()) {
                            String ipaddress = inetAddress.getHostAddress();
                            if (!ipaddress.contains("::") && !ipaddress.contains("0:0:")
                                    && !ipaddress.contains("fe80")) {
                                localIP = ipaddress;
                            }
                        }
                    }
                }
            }
        } catch (SocketException ex) {
            localIP = "127.0.0.1";
            return;
        }
    }

    private void initDockerId() {
        BufferedReader in = null;
        try {
            File file = new File("/proc/self/cgroup");
            if (file.exists() == false) {
                return;
            }
            in = new BufferedReader(new FileReader("/proc/self/cgroup"));
            String dockerID = in.readLine();
            if (dockerID.equals("") == false) {
                int n = dockerID.indexOf("/");
                String dockerID2 = dockerID.substring(n + 1, (dockerID.length() - n - 1));
                n = dockerID2.indexOf("/");
                dockerId = dockerID2.substring(n + 1, 12);
            }
        } catch (IOException e) {
            return;
        } catch (NullPointerException e2) {
            return;
        } catch (Exception e3) {
            return;
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                }
            }
        }
    }
}
