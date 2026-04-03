import random
import sys
import simpy

from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Set
from models import DataBlock, EdgeNode, NodeStatus

try:
    import simpy
    import numpy as np
    import matplotlib.pyplot as plt
    from matplotlib.animation import FuncAnimation
except ImportError as e:
    print(f"Ошибка импорта: {e}")
    print("Установите необходимые библиотеки:")
    print("pip install simpy numpy matplotlib")
    sys.exit(1)

class EdgeStorageSystem:
    """
    Распределённая система хранения данных на периферии.
    Реализует параллельные алгоритмы репликации и восстановления.
    """
    
    def __init__(self, env: simpy.Environment, num_nodes: int, 
                 replication_factor: int, config: Dict, metrics_collector):
        self.env = env
        self.num_nodes = num_nodes
        self.replication_factor = replication_factor
        self.config = config
        self.metrics = metrics_collector
        
        # Создание узлов
        self.nodes: List[EdgeNode] = []
        for i in range(num_nodes):
            node = EdgeNode(i, env, config, metrics_collector)
            self.nodes.append(node)
        
        # Карта размещения реплик: block_id -> список node_id
        self.block_placement: Dict[int, List[int]] = defaultdict(list)
        
        # Счётчик блоков
        self.next_block_id = 0
        
    def _select_nodes_for_replica(self, exclude_nodes: Set[int] = None) -> List[int]:
        """Выбрать узлы для размещения реплик (параллельное размещение)"""
        if exclude_nodes is None:
            exclude_nodes = set()
        
        available_nodes = [n.node_id for n in self.nodes 
                          if n.status == NodeStatus.ONLINE 
                          and n.node_id not in exclude_nodes]
        
        if len(available_nodes) < self.replication_factor:
            # Недостаточно живых узлов - используем что есть
            return available_nodes
        
        # Случайный выбор (в реальном алгоритме здесь была бы оптимизация)
        return random.sample(available_nodes, self.replication_factor)
    
    def write_block(self, data_size: int = 1024) -> Optional[int]:
        """
        Запись блока данных с параллельной репликацией.
        Возвращает block_id или None при ошибке.
        """
        block_id = self.next_block_id
        self.next_block_id += 1
        
        block = DataBlock(block_id, data_size, datetime.now())
        
        # Выбор узлов для реплик
        target_nodes = self._select_nodes_for_replica()
        
        if not target_nodes:
            self.metrics.record_write_failure(block_id, "no_nodes_available")
            return None
        
        # Параллельная запись на все выбранные узлы
        successful_writes = 0
        for node_id in target_nodes:
            if self.nodes[node_id].store_block(block):
                successful_writes += 1
        
        if successful_writes >= self.config.get('min_writes_for_success', 1):
            self.block_placement[block_id] = target_nodes
            self.metrics.record_write_success(block_id, successful_writes, 
                                              len(target_nodes))
            return block_id
        else:
            self.metrics.record_write_failure(block_id, "insufficient_replicas")
            return None
    
    def read_block(self, block_id: int) -> Optional[DataBlock]:
        """Чтение блока данных (с обходом отказавших узлов)"""
        if block_id not in self.block_placement:
            return None
        
        # Параллельный опрос узлов, где могут быть реплики
        for node_id in self.block_placement[block_id]:
            if self.nodes[node_id].status == NodeStatus.ONLINE:
                block = self.nodes[node_id].read_block(block_id)
                if block:
                    self.metrics.record_read_success(block_id, node_id)
                    return block
        
        self.metrics.record_read_failure(block_id)
        return None
    
    def repair_degraded_replicas(self, block_id: int):
        """
        Параллельный алгоритм восстановления недостающих реплик.
        Ключевой алгоритм для диссертации!
        """
        if block_id not in self.block_placement:
            return
        
        current_nodes = set(self.block_placement[block_id])
        online_nodes = {n.node_id for n in self.nodes if n.status == NodeStatus.ONLINE}
        offline_nodes = current_nodes - online_nodes
        
        if not offline_nodes:
            return  # Все реплики в порядке
        
        # Нужно восстановить реплики на offline_nodes
        needed_replicas = len(offline_nodes)
        
        # Поиск живых узлов, не содержащих эту реплику
        available_nodes = [n.node_id for n in self.nodes 
                          if n.status == NodeStatus.ONLINE 
                          and n.node_id not in current_nodes]
        
        # Восстановление (алгоритм параллельного копирования)
        successful_repairs = 0
        for target_node in available_nodes[:needed_replicas]:
            # Копируем данные с любого живого узла, где есть реплика
            source_nodes = list(current_nodes - offline_nodes)
            if source_nodes:
                source = random.choice(source_nodes)
                block = self.nodes[source].read_block(block_id)
                if block and self.nodes[target_node].store_block(block):
                    self.block_placement[block_id].append(target_node)
                    successful_repairs += 1
                    self.metrics.record_repair_success(block_id, source, target_node)
        
        # Удаляем записи о мёртвых узлах
        self.block_placement[block_id] = [n for n in self.block_placement[block_id] 
                                          if n in online_nodes]
        
        return successful_repairs
    
    def get_availability_score(self) -> float:
        """Вычислить текущую доступность данных (0-1)"""
        if not self.block_placement:
            return 1.0
        
        total_blocks = len(self.block_placement)
        available_blocks = 0
        
        for block_id, nodes in self.block_placement.items():
            for node_id in nodes:
                if self.nodes[node_id].status == NodeStatus.ONLINE:
                    available_blocks += 1
                    break
        
        return available_blocks / total_blocks if total_blocks > 0 else 1.0
    
    def get_stats(self) -> Dict:
        """Получить статистику системы"""
        return {
            'num_nodes': self.num_nodes,
            'online_nodes': sum(1 for n in self.nodes if n.status == NodeStatus.ONLINE),
            'total_blocks': len(self.block_placement),
            'availability': self.get_availability_score(),
            'replication_factor': self.replication_factor,
            'node_stats': [n.get_stats() for n in self.nodes]
        }

class AggressiveEnvironment:
    """
    Симулятор агрессивных условий внешней среды.
    Генерирует случайные сбои узлов с заданной интенсивностью.
    """
    
    def __init__(self, env: simpy.Environment, storage_system: EdgeStorageSystem,
                 failure_rate: float, config: Dict, log_callback=None):
        self.env = env
        self.storage = storage_system
        self.failure_rate = failure_rate  # отказов в единицу времени
        self.config = config
        self.log_callback = log_callback
        self.active = True
        
    def run(self):
        """Основной цикл генерации сбоев"""
        while self.active:
            # Время до следующего сбоя (экспоненциальное распределение)
            if self.failure_rate > 0:
                time_to_failure = random.expovariate(self.failure_rate)
                yield self.env.timeout(time_to_failure)
                
                if self.active:
                    # Выбор случайного узла для отказа
                    if self.storage.num_nodes > 0:
                        node_idx = random.randint(0, self.storage.num_nodes - 1)
                        node = self.storage.nodes[node_idx]
                        
                        if node.status == NodeStatus.ONLINE:
                            node.fail()
                            self._log(f"[СБОЙ] Узел {node.node_id} отказал в момент {self.env.now:.1f}")
                            
                            # Планируем восстановление
                            self.env.process(self._schedule_recovery(node))
    
    def _schedule_recovery(self, node: EdgeNode):
        """Планирование восстановления узла"""
        # Время до начала восстановления (задержка обнаружения)
        detection_delay = random.uniform(
            self.config.get('failure_detection_delay', 1.0),
            self.config.get('failure_detection_delay', 1.0) * 2
        )
        yield self.env.timeout(detection_delay)
        
        if self.active and node.status == NodeStatus.OFFLINE:
            self._log(f"[ВОССТАНОВЛЕНИЕ] Узел {node.node_id} начинает восстановление")
            yield self.env.process(node.recover())
            self._log(f"[ВОССТАНОВЛЕНИЕ] Узел {node.node_id} восстановлен в момент {self.env.now:.1f}")
            
            # Запуск параллельного восстановления данных
            self.env.process(self._parallel_data_repair())
    
    def _parallel_data_repair(self):
        """
        Параллельный алгоритм восстановления данных после отказа узла.
        Это ключевая часть, которую вы будете оптимизировать в диссертации.
        """
        self._log(f"[РЕПАР] Запуск параллельного восстановления данных")
        
        # Сбор всех блоков, требующих восстановления
        blocks_to_repair = []
        for block_id, nodes in self.storage.block_placement.items():
            # Проверяем, есть ли живые реплики
            has_live = any(self.storage.nodes[n].status == NodeStatus.ONLINE for n in nodes)
            if not has_live:
                blocks_to_repair.append(block_id)
        
        if blocks_to_repair:
            self._log(f"[РЕПАР] Требуется восстановление {len(blocks_to_repair)} блоков")
            
            # Параллельное восстановление (основной алгоритм)
            repair_tasks = []
            for block_id in blocks_to_repair[:50]:  # Ограничиваем для производительности
                repair_tasks.append(self.env.process(
                    self._repair_single_block(block_id)
                ))
            
            # Ждём завершения параллельных задач
            yield self.env.timeout(0)
            for task in repair_tasks:
                yield task
            
            self._log(f"[РЕПАР] Восстановление завершено")
    
    def _repair_single_block(self, block_id: int):
        """Восстановление одного блока данных"""
        # Симуляция времени восстановления
        repair_time = random.uniform(0.5, 2.0)
        yield self.env.timeout(repair_time)
        
        # В реальном алгоритме здесь было бы копирование данных
        self.storage.repair_degraded_replicas(block_id)
    
    def _log(self, message: str):
        """Логирование с временной меткой"""
        if self.log_callback:
            self.log_callback(f"[{self.env.now:.1f}] {message}")

class EdgeStorageSystem:
    """
    Распределённая система хранения данных на периферии.
    Реализует параллельные алгоритмы репликации и восстановления.
    """
    
    def __init__(self, env: simpy.Environment, num_nodes: int, 
                 replication_factor: int, config: Dict, metrics_collector):
        self.env = env
        self.num_nodes = num_nodes
        self.replication_factor = replication_factor
        self.config = config
        self.metrics = metrics_collector
        
        # Создание узлов
        self.nodes: List[EdgeNode] = []
        for i in range(num_nodes):
            node = EdgeNode(i, env, config, metrics_collector)
            self.nodes.append(node)
        
        # Карта размещения реплик: block_id -> список node_id
        self.block_placement: Dict[int, List[int]] = defaultdict(list)
        
        # Счётчик блоков
        self.next_block_id = 0
        
    def _select_nodes_for_replica(self, exclude_nodes: Set[int] = None) -> List[int]:
        """Выбрать узлы для размещения реплик (параллельное размещение)"""
        if exclude_nodes is None:
            exclude_nodes = set()
        
        available_nodes = [n.node_id for n in self.nodes 
                          if n.status == NodeStatus.ONLINE 
                          and n.node_id not in exclude_nodes]
        
        if len(available_nodes) < self.replication_factor:
            # Недостаточно живых узлов - используем что есть
            return available_nodes
        
        # Случайный выбор (в реальном алгоритме здесь была бы оптимизация)
        return random.sample(available_nodes, self.replication_factor)
    
    def write_block(self, data_size: int = 1024) -> Optional[int]:
        """
        Запись блока данных с параллельной репликацией.
        Возвращает block_id или None при ошибке.
        """
        block_id = self.next_block_id
        self.next_block_id += 1
        
        block = DataBlock(block_id, data_size, datetime.now())
        
        # Выбор узлов для реплик
        target_nodes = self._select_nodes_for_replica()
        
        if not target_nodes:
            self.metrics.record_write_failure(block_id, "no_nodes_available")
            return None
        
        # Параллельная запись на все выбранные узлы
        successful_writes = 0
        for node_id in target_nodes:
            if self.nodes[node_id].store_block(block):
                successful_writes += 1
        
        if successful_writes >= self.config.get('min_writes_for_success', 1):
            self.block_placement[block_id] = target_nodes
            self.metrics.record_write_success(block_id, successful_writes, 
                                              len(target_nodes))
            return block_id
        else:
            self.metrics.record_write_failure(block_id, "insufficient_replicas")
            return None
    
    def read_block(self, block_id: int) -> Optional[DataBlock]:
        """Чтение блока данных (с обходом отказавших узлов)"""
        if block_id not in self.block_placement:
            return None
        
        # Параллельный опрос узлов, где могут быть реплики
        for node_id in self.block_placement[block_id]:
            if self.nodes[node_id].status == NodeStatus.ONLINE:
                block = self.nodes[node_id].read_block(block_id)
                if block:
                    self.metrics.record_read_success(block_id, node_id)
                    return block
        
        self.metrics.record_read_failure(block_id)
        return None
    
    def repair_degraded_replicas(self, block_id: int):
        """
        Параллельный алгоритм восстановления недостающих реплик.
        Ключевой алгоритм для диссертации!
        """
        if block_id not in self.block_placement:
            return
        
        current_nodes = set(self.block_placement[block_id])
        online_nodes = {n.node_id for n in self.nodes if n.status == NodeStatus.ONLINE}
        offline_nodes = current_nodes - online_nodes
        
        if not offline_nodes:
            return  # Все реплики в порядке
        
        # Нужно восстановить реплики на offline_nodes
        needed_replicas = len(offline_nodes)
        
        # Поиск живых узлов, не содержащих эту реплику
        available_nodes = [n.node_id for n in self.nodes 
                          if n.status == NodeStatus.ONLINE 
                          and n.node_id not in current_nodes]
        
        # Восстановление (алгоритм параллельного копирования)
        successful_repairs = 0
        for target_node in available_nodes[:needed_replicas]:
            # Копируем данные с любого живого узла, где есть реплика
            source_nodes = list(current_nodes - offline_nodes)
            if source_nodes:
                source = random.choice(source_nodes)
                block = self.nodes[source].read_block(block_id)
                if block and self.nodes[target_node].store_block(block):
                    self.block_placement[block_id].append(target_node)
                    successful_repairs += 1
                    self.metrics.record_repair_success(block_id, source, target_node)
        
        # Удаляем записи о мёртвых узлах
        self.block_placement[block_id] = [n for n in self.block_placement[block_id] 
                                          if n in online_nodes]
        
        return successful_repairs
    
    def get_availability_score(self) -> float:
        """Вычислить текущую доступность данных (0-1)"""
        if not self.block_placement:
            return 1.0
        
        total_blocks = len(self.block_placement)
        available_blocks = 0
        
        for block_id, nodes in self.block_placement.items():
            for node_id in nodes:
                if self.nodes[node_id].status == NodeStatus.ONLINE:
                    available_blocks += 1
                    break
        
        return available_blocks / total_blocks if total_blocks > 0 else 1.0
    
    def get_stats(self) -> Dict:
        """Получить статистику системы"""
        return {
            'num_nodes': self.num_nodes,
            'online_nodes': sum(1 for n in self.nodes if n.status == NodeStatus.ONLINE),
            'total_blocks': len(self.block_placement),
            'availability': self.get_availability_score(),
            'replication_factor': self.replication_factor,
            'node_stats': [n.get_stats() for n in self.nodes]
        }

class MetricsCollector:
    """Сбор метрик для анализа и визуализации"""
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        self.node_failures = []  # (time, node_id)
        self.node_recoveries = []  # (time, node_id)
        self.write_successes = []  # (time, block_id, replicas_written, target_replicas)
        self.write_failures = []  # (time, block_id, reason)
        self.read_successes = []  # (time, block_id, node_id)
        self.read_failures = []  # (time, block_id)
        self.repair_successes = []  # (time, block_id, source, target)
        
        self.availability_history = []  # (time, score)
        self.health_history = []  # (time, online_nodes, total_nodes)
        
    def record_node_failure(self, node_id: int, time: float):
        self.node_failures.append((time, node_id))
        
    def record_node_recovery(self, node_id: int, time: float):
        self.node_recoveries.append((time, node_id))
        
    def record_write_success(self, block_id: int, replicas_written: int, target_replicas: int):
        self.write_successes.append((block_id, replicas_written, target_replicas))
        
    def record_write_failure(self, block_id: int, reason: str):
        self.write_failures.append((block_id, reason))
        
    def record_read_success(self, block_id: int, node_id: int):
        self.read_successes.append((block_id, node_id))
        
    def record_read_failure(self, block_id: int):
        self.read_failures.append((block_id))
        
    def record_repair_success(self, block_id: int, source_node: int, target_node: int):
        self.repair_successes.append((block_id, source_node, target_node))
        
    def record_availability(self, time: float, score: float):
        self.availability_history.append((time, score))
        
    def record_health(self, time: float, online_nodes: int, total_nodes: int):
        self.health_history.append((time, online_nodes, total_nodes))
        
    def get_summary(self) -> Dict:
        """Получить сводную статистику"""
        return {
            'total_failures': len(self.node_failures),
            'total_recoveries': len(self.node_recoveries),
            'total_writes_success': len(self.write_successes),
            'total_writes_failed': len(self.write_failures),
            'total_reads_success': len(self.read_successes),
            'total_reads_failed': len(self.read_failures),
            'total_repairs': len(self.repair_successes),
            'avg_replicas_per_write': np.mean([w[1] for w in self.write_successes]) if self.write_successes else 0,
            'write_success_rate': len(self.write_successes) / (len(self.write_successes) + len(self.write_failures)) if (self.write_successes or self.write_failures) else 1.0
        }