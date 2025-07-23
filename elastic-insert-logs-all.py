# elasticsearch 7.17.9
from elasticsearch import Elasticsearch
from elasticsearch import AsyncElasticsearch
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import asyncio

class ElasticsearchLogger:
    def __init__(self, 
                 hosts: list = ['http://localhost:9200'], 
                 default_index: str = "mh-logs",
                 timeout: int = 30):
        """
        初始化Elasticsearch日志记录器（支持同步和异步）
        
        参数:
            hosts: Elasticsearch主机地址列表
            default_index: 默认的索引/数据流名称前缀
            timeout: 连接超时时间（秒）
        """
        # 同步客户端
        self.es = Elasticsearch(
            hosts,
            timeout=timeout
        )
        
        # 异步客户端
        self.async_es = AsyncElasticsearch(
            hosts,
            timeout=timeout
        )
        
        self.default_index = default_index
        
    def log(self, 
            message: str, 
            level: str = "INFO", 
            service: str = "python-app", 
            log_type: str = "common",  # 'common' 或 'process'
            # 通用日志字段
            logger: Optional[str] = None, 
            environment: Optional[str] = None,
            # 处理日志字段
            model: Optional[str] = None,
            method: Optional[str] = None,
            action: Optional[str] = None,
            expected_value: Optional[str] = None,
            actual_value: Optional[str] = None,
            result: Optional[str] = None,
            additional_fields: Optional[Dict[str, Any]] = None,
            index: Optional[str] = None) -> Dict:
        """
        同步方式记录日志到Elasticsearch
        
        参数:
            message: 日志消息内容
            level: 日志级别（INFO, DEBUG, ERROR等）
            service: 服务名称
            log_type: 日志类型 ('common' 通用日志 或 'process' 处理日志)
            logger: 记录器名称（通用日志）
            environment: 环境（production, staging等）（通用日志）
            model: 模型名称（处理日志）
            method: 方法名称（处理日志）
            action: 执行的操作（处理日志）
            expected_value: 期望值（处理日志）
            actual_value: 实际值（处理日志）
            result: 结果（处理日志）
            additional_fields: 要包含的额外字段
            index: 覆盖默认的索引/数据流名称
            
        返回:
            Elasticsearch响应字典
        """
        log_entry = self._create_log_entry(
            message=message,
            level=level,
            service=service,
            log_type=log_type,
            logger=logger,
            environment=environment,
            model=model,
            method=method,
            action=action,
            expected_value=expected_value,
            actual_value=actual_value,
            result=result,
            additional_fields=additional_fields
        )
        
        target_index = index or f"{self.default_index}-{log_type}"
        if not target_index:
            raise ValueError("未指定索引且未设置默认索引")
            
        return self.es.index(
            index=target_index,
            document=log_entry
        )
    
    async def log_async(self, 
                       message: str, 
                       level: str = "INFO", 
                       service: str = "python-app", 
                       log_type: str = "common",
                       # 通用日志字段
                       logger: Optional[str] = None, 
                       environment: Optional[str] = None,
                       # 处理日志字段
                       model: Optional[str] = None,
                       method: Optional[str] = None,
                       action: Optional[str] = None,
                       expected_value: Optional[str] = None,
                       actual_value: Optional[str] = None,
                       result: Optional[str] = None,
                       additional_fields: Optional[Dict[str, Any]] = None,
                       index: Optional[str] = None) -> Dict:
        """
        异步方式记录日志到Elasticsearch
        
        参数:
            参数与同步log方法相同
            
        返回:
            Elasticsearch响应字典
        """
        log_entry = self._create_log_entry(
            message=message,
            level=level,
            service=service,
            log_type=log_type,
            logger=logger,
            environment=environment,
            model=model,
            method=method,
            action=action,
            expected_value=expected_value,
            actual_value=actual_value,
            result=result,
            additional_fields=additional_fields
        )
        
        target_index = index or f"{self.default_index}-{log_type}"
        if not target_index:
            raise ValueError("未指定索引且未设置默认索引")
            
        return await self.async_es.index(
            index=target_index,
            document=log_entry
        )
    
    def _create_log_entry(self,
                         message: str,
                         level: str,
                         service: str,
                         log_type: str,
                         # 通用日志字段
                         logger: Optional[str],
                         environment: Optional[str],
                         # 处理日志字段
                         model: Optional[str],
                         method: Optional[str],
                         action: Optional[str],
                         expected_value: Optional[str],
                         actual_value: Optional[str],
                         result: Optional[str],
                         additional_fields: Optional[Dict[str, Any]]) -> Dict:
        """
        创建标准化的日志条目字典
        
        返回:
            格式化后的日志字典
        """
        log_entry = {
            "@timestamp": datetime.now(timezone.utc).isoformat(),
            "message": message,
            "level": level,
            "service": service,
        }
        
        # 根据日志类型添加特定字段
        if log_type == "common":
            if logger is not None:
                log_entry["logger"] = logger
            if environment is not None:
                log_entry["environment"] = environment
        elif log_type == "process":
            if model is not None:
                log_entry["model"] = model
            if method is not None:
                log_entry["method"] = method
            if action is not None:
                log_entry["action"] = action
            if expected_value is not None:
                log_entry["expected_value"] = expected_value
            if actual_value is not None:
                log_entry["actual_value"] = actual_value
            if result is not None:
                log_entry["result"] = result
            elif actual_value is not None and expected_value is not None:
                log_entry["result"] = "success" if actual_value == expected_value else "failure"
        
        if additional_fields:
            log_entry.update(additional_fields)
            
        return log_entry
    
    async def close(self):
        """关闭同步和异步客户端连接"""
        self.es.close()
        await self.async_es.close()

# 示例用法
async def main():
    # 初始化日志记录器
    logger = ElasticsearchLogger(default_index="mh-logs")
    
    try:
        # 通用日志示例
        common_response = logger.log(
            "应用程序启动成功",
            log_type="common",
            logger="main",
            environment="production"
        )
        print("通用同步日志写入结果:", common_response)
        
        common_async_response = logger.log(
            "同步 process 模型验证完成",
            level="INFO",
            log_type="process",
            model="nlp-model-v1",
            method="validate",
            expected_value="0.95",
            actual_value="0.96"
        )
        # 处理日志示例
        process_response = await logger.log_async(
            "模型验证完成",
            level="INFO",
            log_type="process",
            model="nlp-model-v1",
            method="validate",
            expected_value="0.95",
            actual_value="0.96"
        )
        print("处理异步日志写入结果:", process_response)
        
        # 带额外字段的日志
        custom_response = await logger.log_async(
            "自定义日志示例",
            level="DEBUG",
            log_type="common",
            additional_fields={
                "user_id": 123,
                "operation": "数据导出",
                "duration_ms": 245
            }
        )
        print("自定义异步日志写入结果:", custom_response)
        
    finally:
        # 正确清理资源
        await logger.close()

if __name__ == "__main__":
    asyncio.run(main())