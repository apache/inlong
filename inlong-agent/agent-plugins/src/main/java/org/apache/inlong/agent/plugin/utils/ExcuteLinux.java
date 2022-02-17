package org.apache.inlong.agent.plugin.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class ExcuteLinux {

    public static String exeCmd(String commandStr) {

        String result = null;
        try {
            String[] cmd = new String[]{"/bin/sh", "-c",commandStr};
            Process ps = Runtime.getRuntime().exec(cmd);

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            StringBuffer sb = new StringBuffer();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            result = sb.toString();


        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;

    }
}