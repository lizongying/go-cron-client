# go cron client

单服务器，多客户端的定时任务管理工具。方便定时任务的分配、管理、监控。

* 结合go-cron-server使用

### dev

```
go run main.go -c dev.yml
```

### prod

linux build

```
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o cron_client
```

mac build

```
go build -o cron_client
```

run

```
./cron_client -c example.yml
```

### others

* 支持但不鼓励分钟以下定时任务。这种情形比较少见，在脚本中直接处理更加方便，也能与crontab保持兼容。
* 支持当定时任务上次没有结束时不再启动新进程，适用于大多数情况。默认不可修改。
* 暂时不支持多进程执行。这种情形比较少见，在脚本中直接处理更加方便。
* 支持任务设置group，如不设置，所有group都会执行。
* 多个client可以设置同一个group。
* 支持执行目录的配置。
* 推荐使用mongo进行数据存储，防止意外任务丢失。
* server和client的连接时间(interval)，单位秒，默认一秒。
* 简单的集群管理
* 为什么没有使用服务注册发现工具？为了保持简洁性，现阶段暂不支持。
* client重启后任务自动恢复
* client或server重启后自动连接

### todo

* 多server支持、server选举
* ui
