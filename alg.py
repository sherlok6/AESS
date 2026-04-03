import abc
import random
import sys

from models import NodeStatus

if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

class BaseRepairAlgorithm(abc.ABC):
    """Base class for repair algorithms"""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
    
    @abc.abstractmethod
    def repair(self, env, storage_system, failed_node_id: int, log_callback=None):
        pass


class SequentialRepairAlgorithm(BaseRepairAlgorithm):
    """Sequential repair algorithm"""
    
    def __init__(self):
        super().__init__(
            "Sequential",
            "Repairs data blocks one by one. Simple but slow."
        )
    
    def repair(self, env, storage_system, failed_node_id: int, log_callback=None):
        if log_callback:
            log_callback(f"[ALGO] {self.name}: starting repair")
        
        blocks_to_repair = []
        for block_id, nodes in storage_system.block_placement.items():
            if failed_node_id in nodes:
                blocks_to_repair.append(block_id)
        
        if log_callback:
            log_callback(f"[ALGO] Found {len(blocks_to_repair)} blocks to repair")
        
        for block_id in blocks_to_repair:
            repair_time = random.uniform(0.5, 1.5)
            yield env.timeout(repair_time)
            storage_system.repair_degraded_replicas(block_id)
            if log_callback:
                log_callback(f"[ALGO] Repaired block {block_id}")
        
        if log_callback:
            log_callback(f"[ALGO] {self.name}: repair completed")


class ParallelRepairAlgorithm(BaseRepairAlgorithm):
    """Parallel repair algorithm"""
    
    def __init__(self, max_parallel: int = 5):
        super().__init__(
            "Parallel",
            f"Repairs up to {max_parallel} blocks simultaneously."
        )
        self.max_parallel = max_parallel
    
    def repair(self, env, storage_system, failed_node_id: int, log_callback=None):
        if log_callback:
            log_callback(f"[ALGO] {self.name}: starting repair (max_parallel={self.max_parallel})")
        
        blocks_to_repair = []
        for block_id, nodes in storage_system.block_placement.items():
            if failed_node_id in nodes:
                blocks_to_repair.append(block_id)
        
        if log_callback:
            log_callback(f"[ALGO] Found {len(blocks_to_repair)} blocks to repair")
        
        # Create repair tasks and wait for them properly
        for block_id in blocks_to_repair[:50]:
            # Use env.process() and yield from the process
            yield env.process(self._repair_block(env, storage_system, block_id, log_callback))
        
        if log_callback:
            log_callback(f"[ALGO] {self.name}: repair completed")
    
    def _repair_block(self, env, storage_system, block_id: int, log_callback=None):
        repair_time = random.uniform(0.5, 1.5)
        yield env.timeout(repair_time)
        storage_system.repair_degraded_replicas(block_id)


class PriorityBasedRepairAlgorithm(BaseRepairAlgorithm):
    """Priority-based repair algorithm"""
    
    def __init__(self):
        super().__init__(
            "Priority-based",
            "Repairs critical data blocks first."
        )
    
    def repair(self, env, storage_system, failed_node_id: int, log_callback=None):
        if log_callback:
            log_callback(f"[ALGO] {self.name}: starting repair")
        
        blocks_with_priority = []
        for block_id, nodes in storage_system.block_placement.items():
            if failed_node_id in nodes:
                alive_replicas = sum(1 for n in nodes if storage_system.nodes[n].status == NodeStatus.ONLINE)
                priority = 10 - alive_replicas
                blocks_with_priority.append((priority, block_id))
        
        blocks_with_priority.sort(reverse=True)
        
        if log_callback:
            log_callback(f"[ALGO] Found {len(blocks_with_priority)} blocks to repair")
        
        for priority, block_id in blocks_with_priority:
            repair_time = random.uniform(0.3, 1.0)
            yield env.timeout(repair_time)
            storage_system.repair_degraded_replicas(block_id)
            if log_callback:
                log_callback(f"[ALGO] Repaired block {block_id} (priority={priority})")
        
        if log_callback:
            log_callback(f"[ALGO] {self.name}: repair completed")


class AdaptiveParallelRepairAlgorithm(BaseRepairAlgorithm):
    """Adaptive parallel repair algorithm - YOUR ALGORITHM"""
    
    def __init__(self):
        super().__init__(
            "Adaptive Parallel",
            "Adaptively changes parallelism based on system state."
        )
    
    def repair(self, env, storage_system, failed_node_id: int, log_callback=None):
        if log_callback:
            log_callback(f"[ALGO] {self.name}: starting repair (YOUR ALGORITHM)")
        
        # Collect blocks that were on the failed node
        blocks_to_repair = []
        for block_id, nodes in storage_system.block_placement.items():
            if failed_node_id in nodes:
                blocks_to_repair.append(block_id)
        
        if log_callback:
            log_callback(f"[ALGO] Found {len(blocks_to_repair)} blocks to repair")
        
        # Adaptive parallelism based on number of blocks
        adaptive_parallelism = min(10, max(2, len(blocks_to_repair) // 10 + 1))
        if log_callback:
            log_callback(f"[ALGO] Adaptive parallelism: {adaptive_parallelism}")
        
        # Adaptive repair time
        base_repair_time = 0.5
        if len(blocks_to_repair) > 50:
            base_repair_time = 1.0
        elif len(blocks_to_repair) > 20:
            base_repair_time = 0.7
        
        # Create repair tasks list
        repair_tasks = []
        for i, block_id in enumerate(blocks_to_repair[:100]):
            dynamic_delay = base_repair_time * (1 + 0.1 * (i % adaptive_parallelism))
            repair_tasks.append((block_id, dynamic_delay))
        
        # Execute in batches with proper simpy syntax
        for i in range(0, len(repair_tasks), adaptive_parallelism):
            batch = repair_tasks[i:i + adaptive_parallelism]
            # Start all tasks in the batch
            processes = []
            for block_id, delay in batch:
                processes.append(env.process(self._adaptive_repair_block(
                    env, storage_system, block_id, delay, log_callback
                )))
            # Wait for all tasks in this batch to complete
            for proc in processes:
                yield proc
            if log_callback and len(batch) > 0:
                log_callback(f"[ALGO] Completed batch {i//adaptive_parallelism + 1}")
        
        if log_callback:
            log_callback(f"[ALGO] {self.name}: repair completed")
    
    def _adaptive_repair_block(self, env, storage_system, block_id: int, 
                                delay: float, log_callback=None):
        yield env.timeout(delay)
        storage_system.repair_degraded_replicas(block_id)