##GPFS Installation##

The GPFS cluster was created using the instructions found in our Week 4 homework: https://github.com/MIDS-scaling-up/coursework/tree/master/week4/hw/gpfs_setup

The only difference in our configuration was the use of 150GB secondary disks, and the provisioning of 5 nodes (instead of 3) to the cluster. Total size of our GPFS volume was 750GB.

```
slcli vs create --datacenter=sjc01 --domain=fake.net --hostname=gpfs5 --os=REDHAT_6_64 --key=<KEY_NAME> --cpu=2 --memory=4096 --disk=25 --disk=150 --billing=hourly
```

##Obtaining and Unpacking the Data##

The Million Song Dataset files were obtained from: https://www.opensciencedatacloud.org/publicdata/million-song-dataset/

The total download size was 220GB, so we attempted to use the UDR transfer for greater speed. The service was unavailable on the OSDC end, so we used the rsync commands that they provided instead. Because write affinity was enabled, we split the download commands across all 5 servers. Since there were 26 tarballs, one server had to pick up an extra file, so we tested the download of file A on node 1, then split the remaining 25 across nodes. For example, node 1 was responsible for letters B, G, L, Q, and V.

```
cd /gpfs/gpfsfpo
rsync -avzuP publicdata.opensciencedatacloud.org::ark:/31807/osdc-c1c763e4/A.tar.gz ./
rsync -avzuP publicdata.opensciencedatacloud.org::ark:/31807/osdc-c1c763e4/B.tar.gz ./
rsync -avzuP publicdata.opensciencedatacloud.org::ark:/31807/osdc-c1c763e4/G.tar.gz ./
rsync -avzuP publicdata.opensciencedatacloud.org::ark:/31807/osdc-c1c763e4/L.tar.gz ./
rsync -avzuP publicdata.opensciencedatacloud.org::ark:/31807/osdc-c1c763e4/Q.tar.gz ./
rsync -avzuP publicdata.opensciencedatacloud.org::ark:/31807/osdc-c1c763e4/V.tar.gz ./
tar xzf A.tar.gz ; tar xzf B.tar.gz ; tar xzf G.tar.gz ; tar xzf L.tar.gz ; tar xzf Q.tar.gz ; tar xzf V.tar.gz
```

During this process, the GPFS volume began reporting that it had run out of space. However, the `df -h /gpfs/gpfsfpo` command showed that there was still space available. Checking `mmdf /dev/gpfsfpo` revealed that the file system had run out of inodes. The Million Song Dataset lived up to its name and produced a huge number of small data files. The file system had to be adjusted to allow this huge number of files.

```
mmchfs /dev/gpfsfpo -F MaxNumInodes:1536128
```

We also downloaded the "additional files" that include distilled metadata and SQLite databases about the dataset.

```
rsync -avzuP publicdata.opensciencedatacloud.org::ark:/31807/osdc-c1c763e4/AdditionalFiles.tar.gz ./
tar xzf AdditionalFiles.tar.gz
```

With the tar files (approx. 220GB) and the decompressed data files (approx. 280GB) using around 500GB storage, we removed the compressed tar files to give ourselves some breathing room.

```
rm -f *.tar.gz
```

##NFS Setup##

Now that the data set was in place, we had to make the drive mountable by our Spark cluster. We used NFS to establish a remote mount point for those machines. First, packages had to be installed/updated on the servers to allow NFS connections. Kernel updates were also applied to ensure that we had the most recent updates. The kernel updates required a reboot.

```
yum -y install ksh gcc-c++ compat-libstdc++-33 kernel-devel redhat-lsb net-tools libaio
yum -y update kernel
yum -y install portmap
yum -y install nfs-utils
yum -y install nfs nfs-utils
reboot
```

Once the server returned, the nfs service was started.

```
service nfs start
```

We had many clusters running throughout our testing phases, so we had to open access to the NFS mount to many IP ranges. We did this by populating the `/etc/exports` file with the IP ranges where our servers were expecting to connect from.

```
/gpfs/gpfsfpo 1.1.0.0/16(rw,sync)
/gpfs/gpfsfpo 1.2.0.0/16(rw,sync)
/gpfs/gpfsfpo 2.1.0.0/16(rw,sync)
/gpfs/gpfsfpo 2.2.0.0/16(rw,sync)
```

Then, we made NFS aware of the changes

```
exportfs -a
```

Then, on the cluster nodes, we created a mount point directory and mounted the NFS volume there.

```
mkdir /gpfs
mount -t nfs 1.2.3.4:/gpfs/gpfsfpo /gpfs
```

Then we ran a few checks from the cluster node to be sure everything was working as expected.

```
root@br-eve71zsy:/root# cd /gpfs
root@br-eve71zsy:/gpfs# ls
A  AdditionalFiles  B  C  D  E	F  G  H  I  J  K  L  M	N  O  P  Q  R  S  T  U	V  W  X  Y  Z
root@br-eve71zsy:/gpfs# df -h .
Filesystem                    Size  Used Avail Use% Mounted on
1.2.3.4:/gpfs/gpfsfpo  750G  292G  459G  39% /gpfs
```

