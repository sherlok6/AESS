import random
import simpy

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional



@dataclass
class DataBlock:
    """Блок данных"""
    block_id: int
    data_size: int = 1024
    timestamp: datetime = field(default_factory=datetime.now)

@dataclass
class Replica:
    """Информация о реплике"""
    block_id: int
    node_id: int
    is_valid: bool = True

class NodeStatus(Enum):
    """Состояние узла хранения"""
    ONLINE = "online"
    OFFLINE = "offline"
    DEGRADED = "degraded"
    RECOVERING = "recovering"

class EdgeNode:
    """Узел периферийного хранения данных"""
    
    def __init__(self, node_id: int, env: simpy.Environment, 
                 config: Dict, metrics_collector):
        self.node_id = node_id
        self.env = env
        self.config = config
        self.metrics = metrics_collector
        self.status = NodeStatus.ONLINE
        self.data: Dict[int, DataBlock] = {}
        self.replicas: List[int] = []
        self.last_failure_time = None
        self.recovery_time = None
        
    def store_block(self, block: DataBlock) -> bool:
        if self.status != NodeStatus.ONLINE:
            return False
        self.data[block.block_id] = block
        if block.block_id not in self.replicas:
            self.replicas.append(block.block_id)
        return True
    
    def read_block(self, block_id: int) -> Optional[DataBlock]:
        if self.status != NodeStatus.ONLINE:
            return None
        return self.data.get(block_id)
    
    def delete_block(self, block_id: int):
        if block_id in self.data:
            del self.data[block_id]
        if block_id in self.replicas:
            self.replicas.remove(block_id)
    
    def fail(self):
        if self.status == NodeStatus.ONLINE:
            self.status = NodeStatus.OFFLINE
            self.last_failure_time = self.env.now
            self.metrics.record_node_failure(self.node_id, self.env.now)
    
    def recover(self):
        if self.status == NodeStatus.OFFLINE:
            self.status = NodeStatus.RECOVERING
            recovery_delay = random.uniform(
                self.config['min_recovery_time'],
                self.config['max_recovery_time']
            )
            yield self.env.timeout(recovery_delay)
            self.status = NodeStatus.ONLINE
            self.recovery_time = self.env.now
            self.metrics.record_node_recovery(self.node_id, self.env.now)
    
    def get_stats(self) -> Dict:
        return {
            'node_id': self.node_id,
            'status': self.status.value,
            'blocks_count': len(self.data),
            'replicas_count': len(self.replicas),
            'last_failure': self.last_failure_time,
            'last_recovery': self.recovery_time
        }