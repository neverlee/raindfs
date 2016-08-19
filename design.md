# raindfs设计文档

## 主体架构
前端多台master，后端多台volume。
* master服务 由raft协议决定主从，从master定时从主master获取更新群群信息，提供客户端的上传下载接口支持。主master除支持上传下载接口外，还负责管理集群topo，管理volume信息，分配volume
* volume服务 只做数据存储相关接口，定时向master做心跳汇报，从其他volume服务同步volume等

## 程序结构
* go command 式
* master和volume在以后的版本中支持开在一个进程里的同一个http server里。

## master
### 文件存储
存储一个meta file记录已分配的最大volumeid

## volume
### 接口

### 文件存储
一个数据文件夹，数据文件夹下为各个volume的文件夹（每个volume一个文件夹，名为<volume id>.vol），
每个volume文件夹下除存储着所有的数据之外
数据目录目录结构为`/<vid>.vol/<fid>`

### 文件类型
* 块文件
* 索引文件

