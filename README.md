# Run TSO bench in PD
### 编译
```
make
```

### 本地多节点PD server
** 启动 **
```
make pd
```

** 停止 **
```
make cl
```

### 启动本地 tso-bench
```
编译运行taas
make taas CLIENT_NUM=20 CURRENCY_NUM=10 TEST_TIME=5m LOCAL_IP=11.158.168.215:5010 LOG_PATH=tt.log
编译运行tso
make global CLIENT_NUM=20 CURRENCY_NUM=10 TEST_TIME=5m LOCAL_IP=11.158.168.215:5010 LOG_PATH=tt.log
client_num为对应的客户端数目
currency_num为对应的协程数 总协程数目 client_num*currency_num
test_time 为对应时间，注意为time.duration格式
local_ip为测试ip。
log_path为日志路径

```
### failover操作进程
tso kill leader:
bash ctrl.sh  "11.158.168.215:5010;11.158.168.215:5020;11.158.168.215:5030;11.158.168.215:5040;11.158.168.215:5050" kill tso
2 taas 随机kill 一个机器：
bash ctrl.sh  "11.158.168.215:5010;11.158.168.215:5020;11.158.168.215:5030;11.158.168.215:5040;11.158.168.215:5050" kill taas

拉起一个进程：
bash ctrl.sh  "11.158.168.215:5010;11.158.168.215:5020;11.158.168.215:5030;11.158.168.215:5040;11.158.168.215:5050" create tso
bash ctrl.sh  "11.158.168.215:5010;11.158.168.215:5020;11.158.168.215:5030;11.158.168.215:5040;11.158.168.215:5050" create taas

### 查看数据
默认运行一分钟,结果保存在tso_bench.log中,查看最终统计结果
```
grep secdata: tt.log|awk -F '[:|,]' '{print $2,$6,$7,$8,$9}'
```
