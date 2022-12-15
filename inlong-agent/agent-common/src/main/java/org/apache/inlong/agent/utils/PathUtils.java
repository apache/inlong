package org.apache.inlong.agent.utils;

import org.apache.shiro.util.AntPathMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tools to handle various path issue.(e.g. path match、path research)
 */
public class PathUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(PathUtils.class);
    private static final AntPathMatcher MATCHER = new AntPathMatcher();
    static {
        MATCHER.setPathSeparator(File.separator);
    }

    /**
     * Check whether path name are matched ant path regex
     *
     * @param pathStr path string
     * @param patternStr ant regex pattern
     * @return true if matched
     */
    public static boolean antPathMatch(String pathStr, String patternStr) {
        boolean result = MATCHER.match(patternStr, pathStr);
        LOGGER.info("path: {}, pattern: {}, result: {}", pathStr, patternStr, result);
        return result;
    }

    /**
     * Check whether directory name are included in patternStr prefix</br>
     * e.g. "/a/b/1/3/4/5"  are included in "/a/*\/1/3/**\/1.log" prefix
     *
     * @param dirStr directory string
     * @param patternStr ant regex pattern
     * @return true if all match
     */
    public static boolean antPathIncluded(String dirStr, String patternStr) {
        // todo:待实现
        return true;
    }

    /**
     * Find the longest existing directory in the patternStr.Here are some examples:<br/>
     * <ul>
     *   <li>"/tmp/agent/1.txt" -> "/tmp/agent"
     *   <li>"/tmp/agent/*.txt" -> "/tmp/agent"
     *   <li>"/tmp/agent/**\/?.txt" -> "/tmp/agent"
     *   <li>"/tmp/agent" -> "/tmp/agent"
     * </ul>
     *
     * @param patternStr
     * @return
     */
    public static String findRootPath(String patternStr) {
        Path currentPath = Paths.get(patternStr);
        if (!Files.exists(currentPath)) {
            Path parentPath = currentPath.getParent();
            if (parentPath != null) {
                return findRootPath(parentPath.toString());
            }
        }
        if (Files.isDirectory(currentPath)) {
            return patternStr;
        }
        return currentPath.getParent().toString();
    }

    /**
     * Find the common root path for all patternStrs.
     * <ul>
     *   <li>"/tmp/agent/1.txt","/tmp/**\/*.txt","var/run/a.log" -> "/tmp","var/run"
     * </ul>
     *
     * @param patternStrs
     * @return
     */
    public static Set<String> findCommonRootPath(Collection<String> patternStrs) {
        List<String> rootDirs = patternStrs.stream()
                .map(PathUtils::findRootPath)
                .collect(Collectors.toSet())
                .stream()
                .collect(Collectors.toList());
        Set<String> mergedRootDirs = new HashSet<>();
        for (int i = 0; i < rootDirs.size(); i++) {
            String minCommonWatchDir = rootDirs.get(0);
            for (int j = i; j < rootDirs.size(); j++) {
                if (minCommonWatchDir.startsWith(rootDirs.get(j))) {
                    minCommonWatchDir = rootDirs.get(j);
                }
            }
            mergedRootDirs.add(minCommonWatchDir);
        }
        return mergedRootDirs;
    }

}
