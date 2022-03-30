## Setup a Multi-Node Hadoop Cluster

---

Let's setup a multiple node hadoop cluster for distributed computing. In the era of big data, it's physically infeasible to analyse all the data in a single machine/node, therefore we rely on some distributed mechanism to aid us. Apache Hadoop is one of them.

To setup multiple node hadoop cluster, make sure you have `Apache Hadoop` and `Java` installed. Run the following to check `java` version

```bash
java -version
```

If you haven't installed Apache Hadoop, follow [these](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html) instructions.

---

For the purpose of this tutorial, I will be using 3 virtual machines. But the main process is the same, no matter which setup you'd end up having.

---

### 1. Add DNS or Hostname details

At first step, you need to edit `/etc/hosts` on linux and add the `ip address`, `hostname` and `alias` to your master node.

For e.g:

```
10.4.41.81	alakazam.fib.upc.es alakazam
127.0.0.1 localhost
127.0.1.1 bdm
192.168.1.45 localhost
10.4.41.79	abra.fib.upc.es abra
10.4.41.80	kadabra.fib.upc.es kadabra
```

Here, `abra` and `kadabra` are my slave nodes and `alakazam` (my current machine) is my master node.

Verify this step by:

```bash
nslookup abra
```

or

```bash
ping kadabra
```

---

### 2. Edit Hadoop configuration file(s)

You will need to edit hadoop's configuration file(s) on master node.

All the files will be located at `<HADOOP-INSTALL-DIR>/etc/hadoop/`. 

> Note: In my case, HADOOP-INSTALL-DIR=~/BDM_Software/hadoop/


#### 2.1. core-site.xml

Add the following configuration:

```xml
<configuration>
	<property>
		<name>fs.defaultFS</name>
		<value>hdfs://alakazam.fib.upc.es:27000</value>
	</property>
	<property>
		<name>hadoop.tmp.dir</name>
		<value>/home/bdm/BDM_Software/data/hadoop_data</value>
	</property>
</configuration>
```

You can find all the configuration option [here](https://hadoop.apache.org/docs/r2.8.5/hadoop-project-dist/hadoop-common/core-default.xml)

#### 2.2. mapred-site.xml

Add the following configuration:

```xml
<configuration>
	<property>
		<name>mapreduce.framework.name</name>
		<value>yarn</value>
	</property>
</configuration>
```

You can find all the configuration option [here](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml)

#### 2.3. yarn-site.xml

Add the following configuration:

```xml
<configuration>
	<property>
		<name>yarn.nodemanager.aux-services</name>
		<value>mapreduce_shuffle</value>
	</property>

	<property>
		<name>yarn.nodemanager.disk-health-checker.min-healthy-disks</name>
		<value>0</value>
	</property>
</configuration>
```

You can find all the configuration option [here](https://hadoop.apache.org/docs/r2.7.3/hadoop-yarn/hadoop-yarn-common/yarn-default.xml)

---

### 3. Add slave node details

Now, edit the `slaves` configuration file to tell your master node about the workers/slave nodes

For e.g:

```
10.4.41.79
10.4.41.80
```

Just add the IPs for your slave nodes. Also, make sure to do the public-private key exchange (to avoid entering passport again and again).

---

### 4. Sync the configuration setting across all nodes

In order to let the worker/slave nodes know who the master node is, you need to sync all the changes you have down on the master node in previous step to all the slave nodes.

For e.g:

```bash
scp core-site.xml mapred-site.xml yarn-site.xml abra:/home/bdm/BDM_Software/hadoop/etc/hadoop/.
```

This will copy all 3 files to `abra` node.

---

### 5. Format Namenode

Make sure that Hadoop's binaries are in your path

If you aren't sure, run the following command:

```bash
echo 'export PATH="$PATH:~/BDM_Software/hadoop/bin:~/BDM_Software/hadoop/sbin:~/BDM_Software/hbase/bin:~/BDM_Software/mongodb/bin:~/BDM_Software/spark/bin"' >> ~/.bashrc && . ~/.bashrc
```

Now, all the commands will be available to you (use `TAB` to autocomplete)

To format your namenode (master node), run the following command:

```bash
hdfs namenode -format
```

---

### 6. Start DFS

Now, you can start your distributed file system (DFS) via:

```bash
start-dfs.sh
```

Now, browse to `10.4.41.81:9870` and see the details about your DFS. You can see the datanodes as well.

> In case you don't see any datanodes, follow the below mentioned step

---

### 7. Manually start worker nodes

You can manually add your datanodes via running the below mentioned command in your worker/slave nodes' terminal:

```bash
hadoop-deamon.sh start datanode
```

This should start the datanode manually.


---

### 8. Troubleshooting

In case, that didn't help, you may need to clear the data first. (both on master and worker/slave nodes)

```bash
cd ~/BDM_Software/data/hadoop_data/dfs/ 
rm -r */current/*
cd -
```

This will clear/flush the old cluster details. Now, retry the above solutions.

--- 

>Note: This guide was inspired by [this](https://www.youtube.com/watch?v=-YEcJquYsFo) video.