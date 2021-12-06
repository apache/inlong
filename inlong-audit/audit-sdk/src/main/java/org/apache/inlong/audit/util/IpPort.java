package org.apache.inlong.audit.util;

import java.net.InetSocketAddress;
import org.apache.commons.lang3.math.NumberUtils;
import org.jboss.netty.channel.Channel;
public class IpPort {
    public static final String SEPARATOR = ":";
    public final String ip;
    public final int port;
    public final String key;
    public final InetSocketAddress addr;

    /**
     * Constructor
     *
     * @param ip
     * @param port
     */
    public IpPort(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.key = getIpPortKey(ip, port);
        this.addr = new InetSocketAddress(ip, port);
    }

    /**
     *
     * Constructor
     *
     * @param addr
     */
    public IpPort(InetSocketAddress addr) {
        this.ip = addr.getHostName();
        this.port = addr.getPort();
        this.key = getIpPortKey(ip, port);
        this.addr = addr;
    }

    /**
     * get IpPort by key
     *
     * @param  ip
     * @param  port
     * @return
     */
    public static String getIpPortKey(String ip, int port) {
        return ip + ":" + port;
    }

    /**
     * parse sIpPort
     *
     * @param  ipPort
     * @return
     */
    public static IpPort parseIpPort(String ipPort) {
        String[] splits = ipPort.split(SEPARATOR);
        if (splits.length == 2) {
            String strIp = splits[0];
            String strPort = splits[1];
            int port = NumberUtils.toInt(strPort, 0);
            if (port > 0) {
                return new IpPort(strIp, port);
            }
        }
        return null;
    }

    /**
     * parse InetSocketAddress
     *
     * @param  channel
     * @return
     */
    public static InetSocketAddress parseInetSocketAddress(Channel channel) {
        InetSocketAddress destAddr = null;
        if (channel.getRemoteAddress() instanceof InetSocketAddress) {
            destAddr = (InetSocketAddress) channel.getRemoteAddress();
        } else {
            String sendIp = channel.getRemoteAddress().toString();
            destAddr = new InetSocketAddress(sendIp, 0);
        }
        return destAddr;
    }

    /**
     * hashCode
     */
    @Override
    public int hashCode() {
        int result = ip.hashCode();
        result = 31 * result + port;
        return result;
    }

    /**
     * equals
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!(o instanceof IpPort)) {
            return false;
        }

        try {
            IpPort ctp = (IpPort) o;
            if (ip != null && ip.equals(ctp.port) && port == ctp.port) {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    /**
     * toString
     */
    public String toString() {
        return key;
    }
}

