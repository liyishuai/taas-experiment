# File-to-Pseudo Mapping
|Peseudo in paper|File|Function|
|-|-|-|
|line 407-434|client/taas_dispatcher.go| dispatchRequest|
|line 435-443|client/taas_dispatcher.go| "inline implementation"|
|line 445-449|client/taas_dispatcher.go| sndAndRcv|
|line 697-709|pkg/tso/taas.go|generateTaasTSO|
|line 710-714|pkg/tso/taas.go|Initialize|
|line 715-718|pkg/tso/taas.go|reserveTaasLimit|


# Run TSO bench in PD
### 编译
```
make
环境要求：go的版本为1.20
```

### 本地多节点PD server
**启动**
```
make pd
```

**停止**
```
make cl
```

### 启动本地 tso-bench

**启动tidb-pd tso的测试**
```
make global
```

**启动taas测试**
```
编译运行taas客户端
LOCAL_IP=xxxx （自己本机的ip，port为使用的客户端口,默认为5010、5020...）
make taas CLIENT_NUM=20 CURRENCY_NUM=10 TEST_TIME=5m LOCAL_IP=${LOCAL_IP}:5010 LOG_PATH=tt.log
编译运行tso
make global CLIENT_NUM=20 CURRENCY_NUM=10 TEST_TIME=5m LOCAL_IP=${LOCAL_IP}:5010 LOG_PATH=tt.log
client_num为对应的客户端数目
currency_num为对应的协程数 总协程数目 client_num*currency_num
test_time 为对应时间，注意为time.duration格式
local_ip为测试ip。
log_path为日志路径

```
### failover操作进程
tso kill leader:
bash ctrl.sh  "${LOCAL_IP}:5010;${LOCAL_IP}:5020;${LOCAL_IP}:5030;${LOCAL_IP}:5040;${LOCAL_IP}:5050" kill tso
2 taas 随机kill 一个机器：
bash ctrl.sh  "${LOCAL_IP}:5010;${LOCAL_IP}:5020;${LOCAL_IP}:5030;${LOCAL_IP}:5040;${LOCAL_IP}:5050" kill taas
拉起一个进程：
bash ctrl.sh  "${LOCAL_IP}:5010;${LOCAL_IP}:5020;${LOCAL_IP}:5030;${LOCAL_IP}:5040;${LOCAL_IP}:5050" create tso
bash ctrl.sh  "${LOCAL_IP}:5010;${LOCAL_IP}:5020;${LOCAL_IP}:5030;${LOCAL_IP}:5040;${LOCAL_IP}:5050" create taas

### 查看数据
默认运行一分钟,结果保存在tso_bench.log中,查看最终统计结果
```
grep secdata: tt.log|awk -F '[:|,]' '{print $2,$6,$7,$8,$9}'
```
