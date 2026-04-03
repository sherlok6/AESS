import abc
import random

from models import NodeStatus


class BaseRepairAlgorithm(abc.ABC):
    """Базовый класс для всех алгоритмов восстановления"""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
    
    @abc.abstractmethod
    def repair(self, env, storage_system, failed_node_id: int, log_callback=None):
        """
        Алгоритм восстановления данных после отказа узла
        
        Args:
            env: simpy среда
            storage_system: ссылка на EdgeStorageSystem
            failed_node_id: ID отказавшего узла
            log_callback: функция для логирования
        """
        pass


class SequentialRepairAlgorithm(BaseRepairAlgorithm):
    """Последовательный алгоритм восстановления (базовый)"""
    
    def __init__(self):
        super().__init__(
            "Последовательный (Sequential)",
            "Восстанавливает блоки данных один за другим. Простой, но медленный."
        )
    
    def repair(self, env, storage_system, failed_node_id: int, log_callback=None):
        """Последовательное восстановление"""
        if log_callback:
            log_callback(f"[АЛГОРИТМ] {self.name}: начало восстановления")
        
        # Сбор блоков, которые были на отказавшем узле
        blocks_to_repair = []
        for block_id, nodes in storage_system.block_placement.items():
            if failed_node_id in nodes:
                blocks_to_repair.append(block_id)
        
        if log_callback:
            log_callback(f"[АЛГОРИТМ] Найдено {len(blocks_to_repair)} блоков для восстановления")
        
        # Последовательное восстановление
        for block_id in blocks_to_repair:
            # Симуляция времени восстановления одного блока
            repair_time = random.uniform(0.5, 1.5)
            yield env.timeout(repair_time)
            storage_system.repair_degraded_replicas(block_id)
            if log_callback:
                log_callback(f"[АЛГОРИТМ] Восстановлен блок {block_id}")
        
        if log_callback:
            log_callback(f"[АЛГОРИТМ] {self.name}: восстановление завершено")


class ParallelRepairAlgorithm(BaseRepairAlgorithm):
    """Параллельный алгоритм восстановления (улучшенный)"""
    
    def __init__(self, max_parallel: int = 5):
        super().__init__(
            "Параллельный (Parallel)",
            f"Восстанавливает до {max_parallel} блоков одновременно. Быстрее последовательного."
        )
        self.max_parallel = max_parallel
    
    def repair(self, env, storage_system, failed_node_id: int, log_callback=None):
        """Параллельное восстановление"""
        if log_callback:
            log_callback(f"[АЛГОРИТМ] {self.name}: начало восстановления (max_parallel={self.max_parallel})")
        
        # Сбор блоков, которые были на отказавшем узле
        blocks_to_repair = []
        for block_id, nodes in storage_system.block_placement.items():
            if failed_node_id in nodes:
                blocks_to_repair.append(block_id)
        
        if log_callback:
            log_callback(f"[АЛГОРИТМ] Найдено {len(blocks_to_repair)} блоков для восстановления")
        
        # Параллельное восстановление
        repair_tasks = []
        for block_id in blocks_to_repair[:50]:  # Ограничиваем для производительности
            repair_tasks.append(self._repair_block(env, storage_system, block_id, log_callback))
        
        # Ожидание завершения всех параллельных задач
        for task in repair_tasks:
            yield task
        
        if log_callback:
            log_callback(f"[АЛГОРИТМ] {self.name}: восстановление завершено")
    
    def _repair_block(self, env, storage_system, block_id: int, log_callback=None):
        """Восстановление одного блока"""
        repair_time = random.uniform(0.5, 1.5)
        yield env.timeout(repair_time)
        storage_system.repair_degraded_replicas(block_id)


class PriorityBasedRepairAlgorithm(BaseRepairAlgorithm):
    """Приоритетный алгоритм восстановления"""
    
    def __init__(self):
        super().__init__(
            "Приоритетный (Priority-based)",
            "Сначала восстанавливает критически важные блоки данных."
        )
    
    def repair(self, env, storage_system, failed_node_id: int, log_callback=None):
        """Восстановление с приоритетами"""
        if log_callback:
            log_callback(f"[АЛГОРИТМ] {self.name}: начало восстановления")
        
        # Сбор блоков с приоритетами (чем больше реплик потеряно, тем выше приоритет)
        blocks_with_priority = []
        for block_id, nodes in storage_system.block_placement.items():
            if failed_node_id in nodes:
                # Приоритет = количество живых реплик (чем меньше, тем выше приоритет)
                alive_replicas = sum(1 for n in nodes if storage_system.nodes[n].status == NodeStatus.ONLINE)
                priority = 10 - alive_replicas  # Чем меньше живых реплик, тем выше приоритет
                blocks_with_priority.append((priority, block_id))
        
        # Сортировка по приоритету (от большего к меньшему)
        blocks_with_priority.sort(reverse=True)
        
        if log_callback:
            log_callback(f"[АЛГОРИТМ] Найдено {len(blocks_with_priority)} блоков для восстановления")
        
        # Восстановление с учётом приоритета
        for priority, block_id in blocks_with_priority:
            repair_time = random.uniform(0.3, 1.0)  # Более быстрые для приоритетных
            yield env.timeout(repair_time)
            storage_system.repair_degraded_replicas(block_id)
            if log_callback:
                log_callback(f"[АЛГОРИТМ] Восстановлен блок {block_id} (приоритет={priority})")
        
        if log_callback:
            log_callback(f"[АЛГОРИТМ] {self.name}: восстановление завершено")


class AdaptiveParallelRepairAlgorithm(BaseRepairAlgorithm):
    """Адаптивный параллельный алгоритм (ваш алгоритм для диссертации)"""
    
    def __init__(self):
        super().__init__(
            "Адаптивный параллельный (Adaptive Parallel) - РАЗРАБОТАННЫЙ",
            "Адаптивно изменяет степень параллелизма в зависимости от нагрузки и состояния сети. Является основным разработанным алгоритмом в рамках диссертации."
        )
    
    def repair(self, env, storage_system, failed_node_id: int, log_callback=None):
        """
        АДАПТИВНЫЙ ПАРАЛЛЕЛЬНЫЙ АЛГОРИТМ
        Это место для вашей реализации! Здесь должен быть ваш алгоритм.
        """
        if log_callback:
            log_callback(f"[АЛГОРИТМ] {self.name}: начало восстановления (ВАШ АЛГОРИТМ)")
        
        # ================================================================
        # TODO: ЗДЕСЬ ВАША РЕАЛИЗАЦИЯ АЛГОРИТМА ДЛЯ ДИССЕРТАЦИИ
        # ================================================================
        #
        # Текущая реализация - демонстрационная. Замените её на ваш алгоритм.
        #
        # Идеи для улучшения:
        # 1. Анализ текущей загрузки узлов
        # 2. Динамическое изменение степени параллелизма
        # 3. Оптимизация маршрутов передачи данных
        # 4. Предсказание отказов на основе истории
        # 5. Использование кодов коррекции ошибок
        #
        # ================================================================
        
        # Сбор блоков для восстановления
        blocks_to_repair = []
        for block_id, nodes in storage_system.block_placement.items():
            if failed_node_id in nodes:
                blocks_to_repair.append(block_id)
        
        if log_callback:
            log_callback(f"[АЛГОРИТМ] Найдено {len(blocks_to_repair)} блоков для восстановления")
        
        # Адаптивное определение степени параллелизма
        # Чем больше блоков, тем выше параллелизм (но не более 10)
        adaptive_parallelism = min(10, max(2, len(blocks_to_repair) // 10 + 1))
        if log_callback:
            log_callback(f"[АЛГОРИТМ] Адаптивный параллелизм: {adaptive_parallelism}")
        
        # Адаптивное время восстановления (зависит от количества блоков)
        base_repair_time = 0.5
        if len(blocks_to_repair) > 50:
            base_repair_time = 1.0
        elif len(blocks_to_repair) > 20:
            base_repair_time = 0.7
        
        # Параллельное восстановление с адаптацией
        repair_tasks = []
        for i, block_id in enumerate(blocks_to_repair[:100]):
            # Динамическая задержка для имитации адаптации
            dynamic_delay = base_repair_time * (1 + 0.1 * (i % adaptive_parallelism))
            repair_tasks.append(self._adaptive_repair_block(
                env, storage_system, block_id, dynamic_delay, log_callback
            ))
        
        # Запуск параллельных задач с ограничением
        for i in range(0, len(repair_tasks), adaptive_parallelism):
            batch = repair_tasks[i:i + adaptive_parallelism]
            for task in batch:
                yield task
            if log_callback:
                log_callback(f"[АЛГОРИТМ] Завершена группа {i//adaptive_parallelism + 1}")
        
        if log_callback:
            log_callback(f"[АЛГОРИТМ] {self.name}: восстановление завершено")
    
    def _adaptive_repair_block(self, env, storage_system, block_id: int, 
                                delay: float, log_callback=None):
        """Адаптивное восстановление блока"""
        yield env.timeout(delay)
        storage_system.repair_degraded_replicas(block_id)