# Status
Developing

# raindfs
A distributed file system

# 测试
```shell
./raindfs master
./raindfs volume -master 127.0.0.1:10000 -dir v1 -port 20001
./raindfs volume -master 127.0.0.1:10000 -dir v2 -port 20002
./raindfs volume -master 127.0.0.1:10000 -dir v3 -port 20003
```

# TODO
* 检查各个server参数是否完全对应
* 完成master_server基本接口
* 完成switch_server基本接口

# API设计
## 公共
* /ping 检测连接
* /stats/counter
* /stats/memory

rain master
rain volume
rain server

## MasterServer
### 原始api
* /ms/node/join            post 心路node加入
* /ms/node/stats           get  所有node 状态 暂时不需要
* /ms/vol/{vid}            get  获取vid地址
* /ms/vol/_pick            get  获取可写vid
* /ms/stats
### 开放api

## VolumeSeraer
* /vs/vol/{vid}             get vid信息
* /vs/vol/{vid}             put 分配vid
* /vs/vol/{vid}             del 删除vid
* /vs/fs/{vid}/{fid}        put 上传文件
* /vs/fs/{vid}/{fid}        del 删除文件
* /vs/fs/{vid}/{fid}        get 下载文件
* /vs/fs/{vid}/{fid}/_info  get 文件信息
* /vs/stats/disk
* /vs/stats

## SwitchServer
### 原始api
* /ss/fs/{vid}/{fid} get 下载文件
* /ss/fs/{vid}/{fid} put 上传文件
* /ss/fs/{vid}/{fid} post multipart方式上传
* /vs/stats
### 开放api
