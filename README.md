# docker-DAG
1.install docker(ubuntu)
$sudo apt-get update
$sudo apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    software-properties-common
$curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
$sudo apt-key fingerprint 0EBFCD88
$sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
$sudo apt-get update
$sudo apt-get install docker-ce
$sudo usermod -aG docker $USER

2.install nfs服务器
$sudo apt install nfs-kernel-server

$sudo vi /etc/exports
#/etc/exports文件的内容如下：
/nfs-data *(rw,sync,no_subtree_check,no_root_squash)

#创建共享目录
$sudo mkdir -p /nfs-data

#重启nfs服务：
$sudo service nfs-kernel-server restart

#显示已经mount到本机nfs目录的客户端机器。
$sudo showmount -e localhost

#将配置文件中的目录全部重新export一次！无需重启服务。
$sudo exportfs -rv

3.安装nfs客户端工具
$sudo apt install nfs-common

4.创建volume 连接nfs服务器,ip地址指定为nfs服务器ip
$docker volume create --driver local --opt type=nfs --opt o=addr=192.168.33.10,rw --opt device=:/nfs-data --name nfs-volume

5.install mongodb
$sudo apt-get install mongodb

6.install redis
$ sudo apt-get install redis-server
修改/etc/redis.conf文件，注释掉bind 127.0.0.1

7.初始化swarm管理节点,ip地址换为本机ip
$ docker swarm init --listen-addr 192.168.33.10:2377 --advertise-addr 192.168.33.10

8.安装相关工具包
在requirements.txt所在目录中运行下面命令
$ pip install -r requirements.txt

9.构建工作流相关镜像，每个action对应一个镜像，镜像名为action name的小写

10.分别运行服务器、调度器和监控器
$ python run_server.py
$ python run_scheduler.py
$ python run_monitor.py
