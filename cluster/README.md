# "Ambalt" Cluster
## Ambari + Salt = Ambalt

provision "ambalt" machines, with one master

```
slcli vs create -d sjc01 --os CENTOS_6_64 --cpu 2 --memory 16384 --hostname ambalt-master --domain sum.net --disk 100 --key scup_rb
y
slcli vs create -d sjc01 --os CENTOS_6_64 --cpu 4 --memory 16384 --hostname ambalt1 --domain sum.net --disk 100 --key scup_rb
y
slcli vs create -d sjc01 --os CENTOS_6_64 --cpu 4 --memory 16384 --hostname ambalt2 --domain sum.net --disk 100 --key scup_rb
y
slcli vs create -d sjc01 --os CENTOS_6_64 --cpu 4 --memory 16384 --hostname ambalt3 --domain sum.net --disk 100 --key scup_rb
y
slcli vs create -d sjc01 --os CENTOS_6_64 --cpu 4 --memory 16384 --hostname ambalt4 --domain sum.net --disk 100 --key scup_rb
y
```

copy private key used to provision to master and then ssh to master & install salt
```
scp ~/.ssh/id_rsa root@169.54.147.180:~/.ssh/id_rsa
ssh root@169.54.147.180
wget http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
sudo rpm -Uvh epel-release-6*.rpm
yum -y install salt-ssh
yum -y install salt-master
```

# SALT

## salt-ssh on master

Set up a salt ssh roster:
```
vi /etc/salt/roster
```

With content:
```
ambalt1.sum.net:
  host: 169.54.147.167
  user: root
  passwd: <pwd1>
ambalt2.sum.net:
  host: 169.54.147.173
  user: root
  passwd: <pwd2>
ambalt3.sum.net:
  host: 198.23.83.44
  user: root
  passwd: <pwd3>
ambalt4.sum.net:
  host: 169.54.147.170
  user: root
  passwd: <pwd4>
```

Test salt-ssh
```
salt-ssh -i \* test.ping
```
and/or
```
echo "hello" > test.txt
salt-ssh '*' cp.get_file /root/test.txt test.txt
```

# Ambari

Need to convifgure etc/hosts on every node.
First make <code>/etc/hosts</code> on ambalt-master look like this:

```
127.0.0.1 localhost.localdomain localhost
169.54.147.180 ambalt-master.sum.net ambalt-master
169.54.147.167 ambalt1.sum.net ambalt1
169.54.147.173 ambalt2.sum.net ambalt2
198.23.83.44 ambalt3.sum.net ambalt3
169.54.147.170 ambalt4.sum.net ambalt4
```

Then copy <code>/etc/hosts</code> around
```
salt-ssh '*' cp.get_file /etc/hosts /etc/hosts
```

Also copying around private key
```
salt-ssh '*' cp.get_file /root/.ssh/id_rsa /root/.ssh/id_rsa
```

Need to install and start ntpd on all hosts
```
salt-ssh '*' cmd.run 'yum -y install ntp ntpdate ntp-doc'
salt-ssh '*' cmd.run 'chkconfig ntpd on'
salt-ssh '*' cmd.run 'ntpdate pool.ntp.org'
salt-ssh '*' cmd.run '/etc/init.d/ntpd start'
```

Install Ambari on ambalt-master
```
cd /etc/yum.repos.d/
wget http://s3.amazonaws.com/dev.hortonworks.com/ambari/centos6/2.x/BUILDS/2.1.0-1409/ambaribn.repo
yum -y install ambari-server
```

Start ambari server (I just hit enter to accept all option defaults)
```
ambari-server setup
ambari-server start
```

Rename file that will be used in set up later (for whatever reason the Ambari cluster wizard expects ambari.repo, not ambaribn.repo)
```
cp /etc/yum.repos.d/ambaribn.repo /etc/yum.repos.d/ambari.repo
```

Open ambari on a browser <code> http://169.54.147.180:8080 </code> with user = admin, starting password = admin. Then change the password on the UI: users > admin > password

### Install Wizard

Start the install wizard (big button). Give a name to the cluster.


Use HDP 2.3, in Advanced Repository Settings, select redhat 6.

### Set up Target Hosts

In the target hosts field include the FQDN of the ambari hosts from the etc/hosts file (ip fqdn host) we copied to all nodes
```
ambalt1.sum.net
ambalt2.sum.net
ambalt3.sum.net
ambalt4.sum.net
```
In the Host Registration Information field provide the private ssh key used to provision all the machines

### Confirm Hosts

This should work, if not - woops (make sure you installed & started ntpd and changed the name of /etc/yum/repos.d/ambaribn.repo to ambari.repo on ambalt-master)

### Service Selection

Not really sure which we need - We probably over did it for just managing Yarn, Hadoop and obviously Spark:
```
HDFS
YARN + MapReduce2
Tez
Oozie
ZooKeeper
Slider
Spark
Ambari Metrics
```

### Assign Masters

I made <code>ambalt1</code> the master for all services - not sure if this is a good idea or not

### Assign Slaves and Clients

I selected "all" for DataNode, NodeManager, and Client - and none for NFSGateway - again not sure if this is best

### Customize Services

I accepted the custom services

### Install

The install takes about 15 minutes. After it's done hit comlete to set up the cluster.

# Test Cluster

On ambalt1 or whatever node is spark master
```
su spark
cd /usr/hdp/current/spark-client
./bin/spark-submit --class org.apache.spark.examples.SparkPi --master yarn-client lib/spark-examples*.jar 10
```

# Mount gpfs

Mount gpfs on each ambari host
```
salt-ssh '*' cmd.run 'mkdir /gpfs'
salt-ssh '*' cmd.run 'yum -y install nfs-utils nfs-utils-lib'
salt-ssh '*' cmd.run 'mount -t nfs 198.11.206.107:/gpfs/gpfsfpo /gpfs'
```

# Install hdf5 & h5py

## Install hdf5
```
salt-ssh '*' cmd.run 'wget http://pkgs.repoforge.org/rpmforge-release/rpmforge-release-0.5.3-1.el6.rf.x86_64.rpm'
salt-ssh '*' cmd.run 'rpm --import http://apt.sw.be/RPM-GPG-KEY.dag.txt'
salt-ssh '*' cmd.run 'rpm -i rpmforge-release-0.5.3-1.el6.rf.*.rpm'
salt-ssh '*' cmd.run 'yum -y install hdf5 hdf5-devel'
```

## Install pip & h5py
```
salt-ssh '*' cmd.run 'yum install -y python-setuptools'
salt-ssh '*' cmd.run 'easy_install pip'
salt-ssh '*' cmd.run 'pip install Cython'
salt-ssh '*' cmd.run 'pip install h5py'
salt-ssh '*' cmd.run 'pip install unittest2'
```

# All@GPFS -> Summary@HDFS

Now we want to process gpfs data and save to hdfs
On ambalt1, get h52hdfs from somewhere (git or copy & paste)

run h52hdfs.py (make sure to replace with the appropriate hadoop path, in this case textFile path <code>'hdfs://ambalt1.sum.net:8020/h52text/</code>).

```
su spark
spark-submit --master yarn-cluster --num-executors 12 --executor-cores 1 h52hdfs.py
```

This took about an hour and a half with 12 executors

# OneTag Model

## Install git

Only need to do this on the machine you'll be running spark commands on, ambalt1 for now:
```
yum -y install git
```

## Install sbt

```
curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
yum -y install sbt
```

## Get & Build OneTagApp
```
su spark
cd /home/spark
git clone https://github.com/ScalingUpMusic/SUMapplication.git
cd SUMapplication/src/OneTagApp/
sbt package
```

## Run OneTagApp

Make sure OneTagApp/src/main/scala is pointing to the right hdfs location.
In this case <code>hdfs://ambalt1.sum.net:8020/h52text/</code>

```
spark-submit --master yarn-cluster --num-executors 12 --executor-cores 1 --class "OneTagModel" $(find . -name \*one-tag-model*.jar) "uk"
``` 

The above took about 2 min to run.

Trying with different configuraitons

```
spark-submit --master yarn-cluster --num-executors 5 --driver-cores 4 --executor-memory 1G --executor-cores 8 --class "OneTagModel" $(find . -name \*one-tag-model*.jar) "uk"
``` 


# Add New Nodes to Cluster

slcli vs create -d sjc01 --os CENTOS_6_64 --cpu 4 --memory 16384 --hostname ambalt5 --domain sum.net --disk 100 --key scup_rb
y
slcli vs create -d sjc01 --os CENTOS_6_64 --cpu 4 --memory 16384 --hostname ambalt6 --domain sum.net --disk 100 --key scup_rb
y

## Update Salt

Add new nodes to roster on master
```
vi /etc/salt/roster
```

Append content:
```
ambalt5.sum.net:
  host: 169.54.147.186
  user: root
  passwd: <pwd5>
ambalt6.sum.net:
  host: 198.23.83.42
  user: root
  passwd: <pwd6>
```

Test new nodes
```
salt-ssh -i -E '.*[5-6]' test.ping
```

## Update Ambari

### passwordless ssh

Add the new nodes to the etc/hosts file on the master and pass update to every node.

Append content:

```
169.54.147.186 ambalt5.sum.net ambalt5
198.23.83.42 ambalt6.sum.net ambalt6
```

Then copy <code>/etc/hosts</code> around to **ALL** nodes
```
salt-ssh '*' cp.get_file /etc/hosts /etc/hosts
```

Also make sure new nodes have the private key
```
salt-ssh -E '.*[5-6]' cp.get_file /root/.ssh/id_rsa /root/.ssh/id_rsa
```

Also install and start ntpd on new hosts
```
salt-ssh -E '.*[5-6]' cmd.run 'yum -y install ntp ntpdate ntp-doc'
salt-ssh -E '.*[5-6]' cmd.run 'chkconfig ntpd on'
salt-ssh -E '.*[5-6]' cmd.run 'ntpdate pool.ntp.org'
salt-ssh -E '.*[5-6]' cmd.run '/etc/init.d/ntpd start'
```

## Add hosts via Ambari

Here are some pointers: http://hortonworks.com/hadoop-tutorial/using-apache-ambari-add-new-nodes-existing-cluster/

Go to Hosts tab > Actions > Add New Hosts. Add new FQDNs:
```
ambalt5.sum.net
ambalt6.sum.net
```

Copy & paste private key. Register. Success!

Selected all for DataNode, NodeManager, and Client.<br>Next.<br>Next.<br>Deploy->

### Install extra packages on our new hosts

Mount gpfs on new hosts
```
salt-ssh -E '.*[5-6]' cmd.run 'mkdir /gpfs'
salt-ssh -E '.*[5-6]' cmd.run 'yum -y install nfs-utils nfs-utils-lib'
salt-ssh -E '.*[5-6]' cmd.run 'mount -t nfs 198.11.206.107:/gpfs/gpfsfpo /gpfs'
```

Install hdf5 on new hosts
```
salt-ssh -E '.*[5-6]' cmd.run 'wget http://pkgs.repoforge.org/rpmforge-release/rpmforge-release-0.5.3-1.el6.rf.x86_64.rpm'
salt-ssh -E '.*[5-6]' cmd.run 'rpm --import http://apt.sw.be/RPM-GPG-KEY.dag.txt'
salt-ssh -E '.*[5-6]' cmd.run 'rpm -i rpmforge-release-0.5.3-1.el6.rf.*.rpm'
salt-ssh -E '.*[5-6]' cmd.run 'yum -y install hdf5 hdf5-devel'
```

Install pip & h5py
```
salt-ssh -E '.*[5-6]' cmd.run 'yum install -y python-setuptools'
salt-ssh -E '.*[5-6]' cmd.run 'easy_install pip'
salt-ssh -E '.*[5-6]' cmd.run 'pip install Cython'
salt-ssh -E '.*[5-6]' cmd.run 'pip install h5py'
salt-ssh -E '.*[5-6]' cmd.run 'pip install unittest2'
```
