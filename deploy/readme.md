版本一致性：确保 Elasticsearch、Logstash、Kibana 的版本一致（如都为 7.17.0）。

内存要求：Elasticsearch 推荐至少 2G 以上，尤其在生产环境要调高。

生产环境建议：

使用 HTTPS / 认证插件（如 X-Pack）。

日志采集应使用 Filebeat/Fluentd 代替 TCP 流。

多节点分布式部署。

 先获取http://192.168.10.6:5601/登录要输入的token
先登录 elasticsearch容器：
docker exec -it elasticsearch bash
elasticsearch@9f78aa64dc13:~$ bin/elasticsearch-create-enrollment-token --scope kibana
获取token

再登录 kibana容器,获取验证码
docker exec -it kibana bash
kibana@d0abf410dd85:~$ bin/kibana-verification-code