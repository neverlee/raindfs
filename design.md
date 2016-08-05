# raindfs设计文档

## 主体架构
前端多台master，后端多台volume。
* master 负责管理集群topo，管理volume信息，分配volume。提供部分客户端功能
* volume 只做数据存储相关接口，向master汇报

## 程序结构
* git command 式
* master和volume可以开在一个进程里

## master
### 接口
* 锁volume
* 锁volumeServer
* volume heartbeat report报告

## volume
### 接口
* 增加volume
* 删除volume
* 设置volume状态
* 获取一个上传ID
* 上传文件到临时目录(vid, fdata)
* 将文件从临时目录移至volume目录
* 将文件从volume移至临时目录

### 文件存储
数据目录目录结构为`/<vid>.vol/<fid>`和`/<vid>.tmp/<fid>`

### 文件类型
* 块文件
* 索引文件

