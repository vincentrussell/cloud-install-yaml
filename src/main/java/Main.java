import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.FileSystems;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.firstNonNull;

public class Main {

    public static final String DEFAULT_HADOOP_CLUSTER_NAME = "hadoop-cluster";
    private static String UNPACK_LOCATION = System.getProperty("unpack.location","target/servers");

    private static Pattern SERVER_REGEX = Pattern.compile("(?<host>[^:]+)(?::(?<port>\\d+))?");

    public static void main(String[] args) throws IOException {
        final Yaml yaml = new Yaml();
        try (InputStream inputStream = Main.class.getResourceAsStream("/conf/config.yaml")) {
            Map<String,Object> yamlConfig = (Map<String, Object>) yaml.load(inputStream);

            Server nameNode = getSingleServer(yamlConfig,"name-node", 9000);
            List<Server> dataNodes = getServerList(yamlConfig,"data-nodes", null);
            List<Server> zookeepers = getServerList(yamlConfig,"zookeeper-nodes", null);
            Server accumuloMaster = Server.parse((String) yamlConfig.get("accumulo-master"), null);

            doHadoop(yamlConfig,nameNode,dataNodes);
            doZookeeper(yamlConfig,zookeepers);
            doAccumulo(yamlConfig,accumuloMaster,zookeepers,nameNode,dataNodes);
            doBin(yamlConfig);
        }
    }

    @SuppressWarnings("unchecked")
    private static void doAccumulo(Map<String, Object> yamlConfig, Server accumuloMaster, final List<Server> zookeepers, final Server nameNode, List<Server> dataNodes) throws IOException {
        final String cloudInstallLocation = (String) yamlConfig.get("cloud-install-dir");


        writeConfig(UNPACK_LOCATION + "/accumulo/conf/accumulo-site.xml", new HashMap(){{
            put("instance.volumes","hdfs://" + nameNode.getServer() + ":" + nameNode.getPort() + "/accumulo");
            put("instance.zookeeper.host", Joiner.on(",").join(Lists.transform(zookeepers, new Function<Server, String>() {
                @Nullable
                @Override
                public String apply(@Nullable Server input) {
                    return input.toString();
                }
            })));
            put("instance.secret","DEFAULT");
            put("tserver.memory.maps.max","256M");
            put("tserver.memory.maps.native.enabled","true");
            put("tserver.cache.data.size","15M");
            put("tserver.cache.index.size","40M");
            put("trace.token.property.password","secret");
            put("trace.user","root");
            put("tserver.sort.buffer.size","50M");
            put("tserver.walog.max.size","256M");
            put("general.classpaths"," <!-- Accumulo requirements -->\n" +
                    "      "+cloudInstallLocation+"accumulo/lib/accumulo-server.jar,\n" +
                    "      "+cloudInstallLocation+"accumulo/lib/accumulo-core.jar,\n" +
                    "      "+cloudInstallLocation+"accumulo/lib/accumulo-start.jar,\n" +
                    "      "+cloudInstallLocation+"accumulo/lib/accumulo-fate.jar,\n" +
                    "      "+cloudInstallLocation+"accumulo/lib/accumulo-proxy.jar,\n" +
                    "      "+cloudInstallLocation+"accumulo/lib/[^.].*.jar,\n" +
                    "      <!-- ZooKeeper requirements -->\n" +
                    "      "+cloudInstallLocation+"zookeeper/zookeeper[^.].*.jar,\n" +
                    "      <!-- Common Hadoop requirements -->\n" +
                    "      "+cloudInstallLocation+"hadoop/etc/hadoop,\n" +
                    "      <!-- Hadoop 2 requirements -->\n" +
                    "      "+cloudInstallLocation+"hadoop/share/hadoop/common/[^.].*.jar,\n" +
                    "      "+cloudInstallLocation+"hadoop/share/hadoop/common/lib/(?!slf4j)[^.].*.jar,\n" +
                    "      "+cloudInstallLocation+"hadoop/share/hadoop/hdfs/[^.].*.jar,\n" +
                    "      "+cloudInstallLocation+"hadoop/share/hadoop/mapreduce/[^.].*.jar,\n" +
                    "      "+cloudInstallLocation+"hadoop/share/hadoop/yarn/[^.].*.jar,\n" +
                    "      "+cloudInstallLocation+"hadoop/share/hadoop/yarn/lib/jersey.*.jar,\n" +
                    "      <!-- End Hadoop 2 requirements -->\n" +
                    "      <!-- HDP 2.0 requirements --><!--\n" +
                    "      /usr/lib/hadoop/[^.].*.jar,\n" +
                    "      /usr/lib/hadoop/lib/[^.].*.jar,\n" +
                    "      /usr/lib/hadoop-hdfs/[^.].*.jar,\n" +
                    "      /usr/lib/hadoop-mapreduce/[^.].*.jar,\n" +
                    "      /usr/lib/hadoop-yarn/[^.].*.jar,\n" +
                    "      /usr/lib/hadoop-yarn/lib/jersey.*.jar,\n" +
                    "      --><!-- End HDP 2.0 requirements -->\n" +
                    "      <!-- HDP 2.2 requirements --><!--\n" +
                    "      /usr/hdp/current/hadoop-client/[^.].*.jar,\n" +
                    "      /usr/hdp/current/hadoop-client/lib/(?!slf4j)[^.].*.jar,\n" +
                    "      /usr/hdp/current/hadoop-hdfs-client/[^.].*.jar,\n" +
                    "      /usr/hdp/current/hadoop-mapreduce-client/[^.].*.jar,\n" +
                    "      /usr/hdp/current/hadoop-yarn-client/[^.].*.jar,\n" +
                    "      /usr/hdp/current/hadoop-yarn-client/lib/jersey.*.jar,\n" +
                    "      /usr/hdp/current/hive-client/lib/hive-accumulo-handler.jar\n" +
                    "      --><!-- End HDP 2.2 requirements -->\n" +
                    "      <!-- IOP 4.1 requirements --><!--\n" +
                    "      /usr/iop/current/hadoop-client/[^.].*.jar,\n" +
                    "      /usr/iop/current/hadoop-client/lib/(?!slf4j)[^.].*.jar,\n" +
                    "      /usr/iop/current/hadoop-hdfs-client/[^.].*.jar,\n" +
                    "      /usr/iop/current/hadoop-mapreduce-client/[^.].*.jar,\n" +
                    "      /usr/iop/current/hadoop-yarn-client/[^.].*.jar,\n" +
                    "      /usr/iop/current/hadoop-yarn-client/lib/jersey.*.jar,\n" +
                    "      /usr/iop/current/hive-client/lib/hive-accumulo-handler.jar\n" +
                    "      --><!-- End IOP 4.1 requirements -->");
        }});

        File slavesFile = FileSystems.getDefault().getPath(UNPACK_LOCATION + "/accumulo/conf/slaves").toFile();
        slavesFile.delete();
        try (RandomAccessFile f = new RandomAccessFile(slavesFile, "rw")) {
            for (Iterator<Server> serverIterator = dataNodes.iterator(); serverIterator.hasNext();) {
                Server server = serverIterator.next();
                f.write(server.getServer().getBytes());

                if (serverIterator.hasNext()) {
                    f.write("\n".getBytes());
                }
            }
        }

        File mastersFile = FileSystems.getDefault().getPath(UNPACK_LOCATION + "/accumulo/conf/masters").toFile();
        mastersFile.delete();
        try (RandomAccessFile f = new RandomAccessFile(mastersFile, "rw")) {
                f.write(accumuloMaster.getServer().getBytes());
        }

        File monitorFile = FileSystems.getDefault().getPath(UNPACK_LOCATION + "/accumulo/conf/monitor").toFile();
        monitorFile.delete();
        try (RandomAccessFile f = new RandomAccessFile(monitorFile, "rw")) {
            f.write(accumuloMaster.getServer().getBytes());
        }

        File tracersFile = FileSystems.getDefault().getPath(UNPACK_LOCATION + "/accumulo/conf/tracers").toFile();
        tracersFile.delete();
        try (RandomAccessFile f = new RandomAccessFile(tracersFile, "rw")) {
            f.write(accumuloMaster.getServer().getBytes());
        }


    }

    private static void doZookeeper(Map<String, Object> yamlConfig, List<Server> zookeepers) throws IOException {
        final String cloudInstallLocation = (String) yamlConfig.get("cloud-install-dir");
        Properties zooProperties = new Properties();
        zooProperties.setProperty("tickTime","2000");
        zooProperties.setProperty("dataDir",cloudInstallLocation + "/zookeeper/dataDir");
        zooProperties.setProperty("clientPort","2181");
        zooProperties.setProperty("initLimit","10");
        zooProperties.setProperty("syncLimit","5");

        if (zookeepers.size() == 1) {
            //noop
        } else {
            int counter = 1;
            for (Iterator<Server> serverIterator = zookeepers.iterator(); serverIterator.hasNext();) {
                Server server = serverIterator.next();
                zooProperties.setProperty("server." + counter, server.getServer()+":2888:3888");
                counter++;
            }
        }

        File hadoopEnv = FileSystems.getDefault().getPath(UNPACK_LOCATION + "/zookeeper/conf/zoo.conf").toFile();
        try (FileOutputStream outputStream = new FileOutputStream(hadoopEnv)) {
            zooProperties.store(outputStream,"zookeeper properties");
        }
    }

    private static void doBin(Map<String, Object> yamlConfig) throws IOException {
        Map<String, Object> filteredConfig
                = Maps.filterValues(yamlConfig, new Predicate<Object>() {
            @Override
            public boolean apply(@Nullable Object input) {
                return String.class.isInstance(input);
            }
        });

        final String hadoopClusterName = firstNonNull((String) yamlConfig.get("hadoop-cluster-name"), DEFAULT_HADOOP_CLUSTER_NAME);
        filteredConfig.put("hadoop-cluster-name", hadoopClusterName);

        File[] files = FileSystems.getDefault().getPath(UNPACK_LOCATION + "/bin").toFile().listFiles();


        for (File file : files) {
            String contents = FileUtils.readFileToString(file);
            file.delete();
            MustacheFactory mf = new DefaultMustacheFactory();
            Mustache mustache = mf.compile(new StringReader(contents), "name");
            try (FileWriter fileWriter = new FileWriter(file)) {
                mustache.execute(fileWriter,filteredConfig);
            }
            file.setExecutable(true);
        }
    }

    private static Server getSingleServer(Map<String, Object> yamlConfig, String key, Integer defaultPort) {
        return Server.parse((String) yamlConfig.get(key), defaultPort);
    }

    private static Server getSingleServer(Map<String, Object> yamlConfig, String key, Integer defaultPort, String defaultValue) {
        return Server.parse((String) yamlConfig.get(key), defaultPort, defaultValue);
    }

    @SuppressWarnings("unchecked")
    private static void doHadoop(final Map<String, Object> yamlConfig, final Server nameNode, final List<Server> dataNodes) throws IOException {
        final String cloudInstallLocation = (String) yamlConfig.get("cloud-install-dir");
        final String javaHome = (String) yamlConfig.get("java-home");
        final String dfsReplication = (String) yamlConfig.get("dfs.replication");


         writeConfig(UNPACK_LOCATION + "/hadoop/etc/hadoop/core-site.xml", new HashMap(){{
             put("fs.defaultFS","hdfs://" + nameNode.getServer() + ":" + nameNode.getPort());
         }});

        writeConfig(UNPACK_LOCATION + "/hadoop/etc/hadoop/hdfs-site.xml", new HashMap(){{
            put("dfs.replication",dfsReplication!=null? dfsReplication : "3");
            put("dfs.namenode.name.dir",cloudInstallLocation + "/hadoop/nameNodeDir");
            put("dfs.datanode.data.dir",cloudInstallLocation + "/hadoop/dataDir");
        }});

        final Server resourceManager = getSingleServer(yamlConfig,"yarn.resourcemanager.hostname",8032,nameNode.getServer());

        writeConfig(UNPACK_LOCATION + "/hadoop/etc/hadoop/yarn-site.xml", new HashMap(){{
            put("yarn.nodemanager.aux-services","mapreduce_shuffle");
            put("yarn.resourcemanager.hostname", resourceManager.getServer().toString());

        }});

        final Server jobTracker = getSingleServer(yamlConfig,"mapreduce.jobtracker.address", 8021, nameNode.getServer());


        writeConfig(UNPACK_LOCATION + "/hadoop/etc/hadoop/mapred-site.xml", new HashMap(){{
            put("mapreduce.jobtracker.address", jobTracker.getServer() + ":" + jobTracker.getPort());
            put("yarn.app.mapreduce.am.resource.mb","1024");
            put("yarn.app.mapreduce.am.command-opts","-Xmx768m");
            put("mapreduce.framework.name","yarn");
            put("mapreduce.map.cpu.vcores","1");
            put("mapreduce.reduce.cpu.vcores","1");
            put("mapreduce.map.memory.mb","1024");
            put("mapreduce.map.java.opts","-Xmx768m");
            put("mapreduce.reduce.memory.mb","1024");
            put("mapreduce.reduce.java.opts","-Xmx768m");
        }});


        File hadoopEnv = FileSystems.getDefault().getPath(UNPACK_LOCATION + "/hadoop/etc/hadoop/hadoop-env.sh").toFile();
        try (RandomAccessFile f = new RandomAccessFile(hadoopEnv, "rws")) {
            byte[] text = new byte[(int) f.length()];
            f.readFully(text);
            f.seek(0); // to the beginning
            f.write(("" +
                    "export HADOOP_PREFIX=\""+cloudInstallLocation+"/hadoop\"\n" +
                    "export HADOOP_HOME=$HADOOP_PREFIX\n" +
                    "export HADOOP_COMMON_HOME=$HADOOP_PREFIX\n" +
                    "export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop\n" +
                    "export HADOOP_HDFS_HOME=$HADOOP_PREFIX\n" +
                    "export HADOOP_MAPRED_HOME=$HADOOP_PREFIX\n" +
                    "export HADOOP_YARN_HOME=$HADOOP_PREFIX\n").getBytes());

            if (javaHome!=null) {
                f.write(("export JAVA_HOME=\""+javaHome+"\"\n").getBytes());
            }

            f.write("\n".getBytes());
            f.write(text);
        }

        File slavesFile = FileSystems.getDefault().getPath(UNPACK_LOCATION + "/hadoop/etc/hadoop/slaves").toFile();
        slavesFile.delete();
        try (RandomAccessFile f = new RandomAccessFile(slavesFile, "rw")) {
            for (Iterator<Server> serverIterator = dataNodes.iterator(); serverIterator.hasNext();) {
                Server server = serverIterator.next();
                f.write(server.getServer().getBytes());

                if (serverIterator.hasNext()) {
                    f.write("\n".getBytes());
                }
            }
        }
    }

    private static void writeConfig(String location, Map<String,String> configMap) throws IOException {
        Configuration config = new Configuration();
        config.clear();
        for (Map.Entry<String,String> entry : configMap.entrySet()) {
            config.set(entry.getKey(),entry.getValue());
        }

        File file = FileSystems.getDefault().getPath(location).toFile();
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            config.writeXml(fileOutputStream);
        }
    }

    private static List<Server> getServerList(Map<String, Object> yamlConfig, String key, final Integer defaultPort) {
        return Lists.transform((List<String>) yamlConfig.get(key), new Function<String, Server>() {
            @Override
            public Server apply(String s) {
                return Server.parse(s, defaultPort);
            }
        });
    }

    private static class Server {
        private final String server;
        private final Integer port;

        private Server(String server, Integer port) {
            this.server = server;
            this.port = port;
        }

        private static Server parse(String string, Integer defaultPort) {
            return parse(string,defaultPort,null);
        }

        public String getServer() {
            return server;
        }

        public Integer getPort() {
            return port;
        }

        private static Server parse(String string, Integer defaultPort, String defaultValue) {

            if (string == null && defaultValue != null) {
                string = defaultValue;
            }

            if (string==null) {
                throw new RuntimeException("could not parse server");
            }

            Matcher matcher = SERVER_REGEX.matcher(string);
            if (matcher.matches()) {
                String server = matcher.group(1);
                String port = matcher.group(2);

                if (port ==null && defaultPort!=null) {
                    port = defaultPort.toString();
                }

                return new Server(server, port!=null ? Integer.parseInt(port) : null );
            } else {
                throw new RuntimeException("could not parse server");
            }
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(server);
            if (port != null) {
                stringBuilder.append(":");
                stringBuilder.append(port);
            }
            return stringBuilder.toString();
        }
    }

}
