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

