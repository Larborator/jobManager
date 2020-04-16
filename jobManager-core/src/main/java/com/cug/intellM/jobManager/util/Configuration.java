package com.cug.intellM.jobManager.util;

import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Created by zsm on 2018/10/9.
 * Configuration 提供多级JSON配置信息无损存储
 */
public class Configuration {

    private static final Pattern VARIABLE_PATTERN = Pattern
            .compile("(\\$)\\{?(\\w+)\\}?");
    private Object root = null;

    /**
     * 从JSON字符串加载Configuration
     */
    public static Configuration from(String json) {
        json = replaceVariable(json);
        checkJSON(json);

        try {
            return new Configuration(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 从包括json的File对象加载Configuration
     */
    public static Configuration from(File file) {
        try {
            return Configuration.from(IOUtils.toString(new FileInputStream(file)));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(
                    String.format("配置信息错误，您提供的配置文件[%s]不存在. 请检查您的配置文件.", file.getAbsolutePath()));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("配置信息错误. 您提供配置文件[%s]读取失败，错误原因: %s. 请检查您的配置文件的权限设置.",
                            file.getAbsolutePath(), e));
        }
    }

    private void checkPath(final String path) {
        if (null == path) {
            throw new IllegalArgumentException(
                    "系统编程错误, 该异常代表系统编程错误, 请联系开发团队!.");
        }

        for (final String each : StringUtils.split(".")) {
            if (StringUtils.isBlank(each)) {
                throw new IllegalArgumentException(String.format(
                        "系统编程错误, 路径[%s]不合法, 路径层次之间不能出现空白字符 .", path));
            }
        }
    }

    private static void checkJSON(final String json) {
        if (StringUtils.isBlank(json)) {
            throw new RuntimeException(
                    "配置信息错误. 因为您提供的配置信息不是合法的JSON格式, JSON不能为空白. 请按照标准json格式提供配置信息. ");
        }
    }

    public static String replaceVariable(final String param) {
        Map<String, String> mapping = new HashMap<String, String>();

        Matcher matcher = VARIABLE_PATTERN.matcher(param);
        while (matcher.find()) {
            String variable = matcher.group(2);
            String value = System.getProperty(variable);
            if (StringUtils.isBlank(value)) {
                value = matcher.group();
            }
            mapping.put(matcher.group(), value);
        }

        String retString = param;
        for (final String key : mapping.keySet()) {
            retString = retString.replace(key, mapping.get(key));
        }

        return retString;
    }

    private Configuration(final String json) {
        try {
            this.root = JSON.parse(json);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("配置信息错误. 您提供的配置信息不是合法的JSON格式: %s . 请按照标准json格式提供配置信息. ", e.getMessage()));
        }
    }

    /**
     * 根据用户提供的json path，寻址具体的对象。
     * <p/>
     * <br>
     * <p/>
     * NOTE: 目前仅支持Map以及List下标寻址, 例如:
     * <p/>
     * <br />
     * <p/>
     * 对于如下JSON
     * <p/>
     * {"a": {"b": {"c": [0,1,2,3]}}}
     * <p/>
     * config.get("") 返回整个Map <br>
     * config.get("a") 返回a下属整个Map <br>
     * config.get("a.b.c") 返回c对应的数组List <br>
     * config.get("a.b.c[0]") 返回数字0
     *
     * @return Java表示的JSON对象，如果path不存在或者对象不存在，均返回null。
     */
    public Object get(final String path) {
        this.checkPath(path);
        try {
            return this.findObject(path);
        } catch (Exception e) {
            return null;
        }
    }


    /**
     * 根据用户提供的json path，插入指定对象，并返回之前存在的对象(如果存在)
     * <p/>
     * <br>
     * <p/>
     * 目前仅支持.以及数组下标寻址, 例如:
     * <p/>
     * <br />
     * <p/>
     * config.set("a.b.c[3]", object);
     * <p/>
     * <br>
     * 对于插入对象，Configuration不做任何限制，但是请务必保证该对象是简单对象(包括Map<String,
     * Object>、List<Object>)，不要使用自定义对象，否则后续对于JSON序列化等情况会出现未定义行为。
     *
     * @param path
     *            JSON path对象
     * @param object
     *            需要插入的对象
     * @return Java表示的JSON对象
     */
    public Object set(final String path, final Object object) {
        checkPath(path);

        Object result = this.get(path);

        setObject(path, extractConfiguration(object));

        return result;
    }

    /**
     * 格式化Configuration输出
     */
    public String beautify() {
        return JSON.toJSONString(this.getInternal(),
                SerializerFeature.PrettyFormat);
    }


    private Object findObject(final String path) {
        boolean isRootQuery = StringUtils.isBlank(path);
        if (isRootQuery) {
            return this.root;
        }

        Object target = this.root;

        for (final String each : split2List(path)) {
            if (isPathMap(each)) {
                target = findObjectInMap(target, each);
                continue;
            } else {
                target = findObjectInList(target, each);
                continue;
            }
        }

        return target;
    }

    private Object findObjectInMap(final Object target, final String index) {
        boolean isMap = (target instanceof Map);
        if (!isMap) {
            throw new IllegalArgumentException(String.format(
                    "您提供的配置文件有误. 路径[%s]需要配置Json格式的Map对象，但该节点发现实际类型是[%s]. 请检查您的配置并作出修改.",
                    index, target.getClass().toString()));
        }

        Object result = ((Map<String, Object>) target).get(index);
        if (null == result) {
            throw new IllegalArgumentException(String.format(
                    "您提供的配置文件有误. 路径[%s]值为null，datax无法识别该配置. 请检查您的配置并作出修改.", index));
        }

        return result;
    }

    private Object findObjectInList(final Object target, final String each) {
        boolean isList = (target instanceof List);
        if (!isList) {
            throw new IllegalArgumentException(String.format(
                    "您提供的配置文件有误. 路径[%s]需要配置Json格式的Map对象，但该节点发现实际类型是[%s]. 请检查您的配置并作出修改.",
                    each, target.getClass().toString()));
        }

        String index = each.replace("[", "").replace("]", "");
        if (!StringUtils.isNumeric(index)) {
            throw new IllegalArgumentException(
                    String.format(
                            "系统编程错误，列表下标必须为数字类型，但该节点发现实际类型是[%s] ，该异常代表系统编程错误, 请联系DataX开发团队 !",
                            index));
        }

        return ((List<Object>) target).get(Integer.valueOf(index));
    }

    private List<String> split2List(final String path) {
        return Arrays.asList(StringUtils.split(split(path), "."));
    }

    private String split(final String path) {
        return StringUtils.replace(path, "[", ".[");
    }

    private boolean isPathMap(final String path) {
        return StringUtils.isNotBlank(path) && !isPathList(path);
    }

    private boolean isPathList(final String path) {
        return path.contains("[") && path.contains("]");
    }

    private void setObject(final String path, final Object object) {
        Object newRoot = setObjectRecursive(this.root, split2List(path), 0,
                object);

        if (isSuitForRoot(newRoot)) {
            this.root = newRoot;
            return;
        }

        throw new RuntimeException(
                String.format("值[%s]无法适配您提供[%s]， 该异常代表系统编程错误, 请联系开发团队!",
                        ToStringBuilder.reflectionToString(object), path));
    }

    private boolean isSuitForRoot(final Object object) {
        if (null != object && (object instanceof List || object instanceof Map)) {
            return true;
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    Object setObjectRecursive(Object current, final List<String> paths,
                              int index, final Object value) {

        // 如果是已经超出path，我们就返回value即可，作为最底层叶子节点
        boolean isLastIndex = index == paths.size();
        if (isLastIndex) {
            return value;
        }

        String path = paths.get(index).trim();
        boolean isNeedMap = isPathMap(path);
        if (isNeedMap) {
            Map<String, Object> mapping;

            // 当前不是map，因此全部替换为map，并返回新建的map对象
            boolean isCurrentMap = current instanceof Map;
            if (!isCurrentMap) {
                mapping = new HashMap<String, Object>();
                mapping.put(
                        path,
                        buildObject(paths.subList(index + 1, paths.size()),
                                value));
                return mapping;
            }

            // 当前是map，但是没有对应的key，也就是我们需要新建对象插入该map，并返回该map
            mapping = ((Map<String, Object>) current);
            boolean hasSameKey = mapping.containsKey(path);
            if (!hasSameKey) {
                mapping.put(
                        path,
                        buildObject(paths.subList(index + 1, paths.size()),
                                value));
                return mapping;
            }

            // 当前是map，而且还竟然存在这个值，好吧，继续递归遍历
            current = mapping.get(path);
            mapping.put(path,
                    setObjectRecursive(current, paths, index + 1, value));
            return mapping;
        }

        boolean isNeedList = isPathList(path);
        if (isNeedList) {
            List<Object> lists;
            int listIndexer = getIndex(path);

            // 当前是list，直接新建并返回即可
            boolean isCurrentList = current instanceof List;
            if (!isCurrentList) {
                lists = expand(new ArrayList<Object>(), listIndexer + 1);
                lists.set(
                        listIndexer,
                        buildObject(paths.subList(index + 1, paths.size()),
                                value));
                return lists;
            }

            // 当前是list，但是对应的indexer是没有具体的值，也就是我们新建对象然后插入到该list，并返回该List
            lists = (List<Object>) current;
            lists = expand(lists, listIndexer + 1);

            boolean hasSameIndex = lists.get(listIndexer) != null;
            if (!hasSameIndex) {
                lists.set(
                        listIndexer,
                        buildObject(paths.subList(index + 1, paths.size()),
                                value));
                return lists;
            }

            // 当前是list，并且存在对应的index，没有办法继续递归寻找
            current = lists.get(listIndexer);
            lists.set(listIndexer,
                    setObjectRecursive(current, paths, index + 1, value));
            return lists;
        }

        throw new RuntimeException(
                "该异常代表系统编程错误, 请联系开发团队 !");
    }

    Object buildObject(final List<String> paths, final Object object) {
        if (null == paths) {
            throw new RuntimeException(
                    "Path不能为null，该异常代表系统编程错误, 请联系DataX开发团队 !");
        }

        if (1 == paths.size() && StringUtils.isBlank(paths.get(0))) {
            return object;
        }

        Object child = object;
        for (int i = paths.size() - 1; i >= 0; i--) {
            String path = paths.get(i);

            if (isPathMap(path)) {
                Map<String, Object> mapping = new HashMap<String, Object>();
                mapping.put(path, child);
                child = mapping;
                continue;
            }

            if (isPathList(path)) {
                List<Object> lists = new ArrayList<Object>(
                        this.getIndex(path) + 1);
                expand(lists, this.getIndex(path) + 1);
                lists.set(this.getIndex(path), child);
                child = lists;
                continue;
            }

            throw new RuntimeException(String.format(
                            "路径[%s]出现非法值类型[%s]，该异常代表系统编程错误, 请联系DataX开发团队! .",
                            StringUtils.join(paths, "."), path));
        }

        return child;
    }

    private int getIndex(final String index) {
        return Integer.valueOf(index.replace("[", "").replace("]", ""));
    }

    private List<Object> expand(List<Object> list, int size) {
        int expand = size - list.size();
        while (expand-- > 0) {
            list.add(null);
        }
        return list;
    }

    @SuppressWarnings("unchecked")
    private Object extractConfiguration(final Object object) {
        if (object instanceof Configuration) {
            return extractFromConfiguration(object);
        }

        if (object instanceof List) {
            List<Object> result = new ArrayList<Object>();
            for (final Object each : (List<Object>) object) {
                result.add(extractFromConfiguration(each));
            }
            return result;
        }

        if (object instanceof Map) {
            Map<String, Object> result = new HashMap<String, Object>();
            for (final String key : ((Map<String, Object>) object).keySet()) {
                result.put(key,
                        extractFromConfiguration(((Map<String, Object>) object)
                                .get(key)));
            }
            return result;
        }

        return object;
    }

    private Object extractFromConfiguration(final Object object) {
        if (object instanceof Configuration) {
            return ((Configuration) object).getInternal();
        }

        return object;
    }
    public Object getInternal() {
        return this.root;
    }

    private static String toJSONString(final Object object) {
        return JSON.toJSONString(object);
    }
    /**
     * 将Configuration作为JSON输出
     */
    public String toJSON() {
        return Configuration.toJSONString(this.getInternal());
    }

    /**
     * 合并其他Configuration，并修改两者冲突的KV配置
     *
     * @param another
     *            合并加入的第三方Configuration
     * @param updateWhenConflict
     *            当合并双方出现KV冲突时候，选择更新当前KV，或者忽略该KV
     * @return 返回合并后对象
     */
    public Configuration merge(final Configuration another,
                               boolean updateWhenConflict) {
        Set<String> keys = another.getKeys();

        for (final String key : keys) {
            // 如果使用更新策略，凡是another存在的key，均需要更新
            if (updateWhenConflict) {
                this.set(key, another.get(key));
                continue;
            }

            // 使用忽略策略，只有another Configuration存在但是当前Configuration不存在的key，才需要更新
            boolean isCurrentExists = this.get(key) != null;
            if (isCurrentExists) {
                continue;
            }

            this.set(key, another.get(key));
        }
        return this;
    }
    /**
     * 获取Configuration下所有叶子节点的key
     * <p/>
     * <br>
     * <p/>
     * 对于<br>
     * <p/>
     * {"a": {"b": {"c": [0,1,2,3]}}, "x": "y"}
     * <p/>
     * 下属的key包括: a.b.c[0],a.b.c[1],a.b.c[2],a.b.c[3],x
     */
    public Set<String> getKeys() {
        Set<String> collect = new HashSet<String>();
        this.getKeysRecursive(this.getInternal(), "", collect);
        return collect;
    }

    @SuppressWarnings("unchecked")
    void getKeysRecursive(final Object current, String path, Set<String> collect) {
        boolean isRegularElement = !(current instanceof Map || current instanceof List);
        if (isRegularElement) {
            collect.add(path);
            return;
        }

        boolean isMap = current instanceof Map;
        if (isMap) {
            Map<String, Object> mapping = ((Map<String, Object>) current);
            for (final String key : mapping.keySet()) {
                if (StringUtils.isBlank(path)) {
                    getKeysRecursive(mapping.get(key), key.trim(), collect);
                } else {
                    getKeysRecursive(mapping.get(key), path + "." + key.trim(),
                            collect);
                }
            }
            return;
        }
    }
}
