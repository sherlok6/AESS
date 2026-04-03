import csv
import os
import platform
import random
import subprocess
import sys
import simpy

from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from alg import BaseRepairAlgorithm
from models import DataBlock, EdgeNode, NodeStatus

try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.chart import LineChart, Reference
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False
    print("Предупреждение: openpyxl не установлен. Экспорт в Excel будет недоступен.")
    print("Установите: pip install openpyxl")

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
    def __init__(self, env: simpy.Environment, num_nodes: int, 
                 replication_factor: int, config: Dict, metrics_collector):
        self.env = env
        self.num_nodes = num_nodes
        self.replication_factor = replication_factor
        self.config = config
        self.metrics = metrics_collector
        
        self.nodes: List[EdgeNode] = []
        for i in range(num_nodes):
            node = EdgeNode(i, env, config, metrics_collector)
            self.nodes.append(node)
        
        self.block_placement: Dict[int, List[int]] = defaultdict(list)
        self.next_block_id = 0
        
    def _select_nodes_for_replica(self, exclude_nodes: Set[int] = None) -> List[int]:
        if exclude_nodes is None:
            exclude_nodes = set()
        
        available_nodes = [n.node_id for n in self.nodes 
                          if n.status == NodeStatus.ONLINE 
                          and n.node_id not in exclude_nodes]
        
        if len(available_nodes) < self.replication_factor:
            return available_nodes
        
        return random.sample(available_nodes, self.replication_factor)
    
    def write_block(self, data_size: int = 1024) -> Optional[int]:
        block_id = self.next_block_id
        self.next_block_id += 1
        
        block = DataBlock(block_id, data_size, datetime.now())
        target_nodes = self._select_nodes_for_replica()
        
        if not target_nodes:
            self.metrics.record_write_failure(block_id, "no_nodes_available")
            return None
        
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
        if block_id not in self.block_placement:
            return None
        
        for node_id in self.block_placement[block_id]:
            if self.nodes[node_id].status == NodeStatus.ONLINE:
                block = self.nodes[node_id].read_block(block_id)
                if block:
                    self.metrics.record_read_success(block_id, node_id)
                    return block
        
        self.metrics.record_read_failure(block_id)
        return None
    
    def repair_degraded_replicas(self, block_id: int):
        if block_id not in self.block_placement:
            return
        
        current_nodes = set(self.block_placement[block_id])
        online_nodes = {n.node_id for n in self.nodes if n.status == NodeStatus.ONLINE}
        offline_nodes = current_nodes - online_nodes
        
        if not offline_nodes:
            return
        
        needed_replicas = len(offline_nodes)
        available_nodes = [n.node_id for n in self.nodes 
                          if n.status == NodeStatus.ONLINE 
                          and n.node_id not in current_nodes]
        
        for target_node in available_nodes[:needed_replicas]:
            source_nodes = list(current_nodes - offline_nodes)
            if source_nodes:
                source = random.choice(source_nodes)
                block = self.nodes[source].read_block(block_id)
                if block and self.nodes[target_node].store_block(block):
                    self.block_placement[block_id].append(target_node)
                    self.metrics.record_repair_success(block_id, source, target_node)
        
        self.block_placement[block_id] = [n for n in self.block_placement[block_id] 
                                          if n in online_nodes]
        return
    
    def get_availability_score(self) -> float:
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
        return {
            'num_nodes': self.num_nodes,
            'online_nodes': sum(1 for n in self.nodes if n.status == NodeStatus.ONLINE),
            'total_blocks': len(self.block_placement),
            'availability': self.get_availability_score(),
            'replication_factor': self.replication_factor,
            'node_stats': [n.get_stats() for n in self.nodes]
        }

class AggressiveEnvironment:
    def __init__(self, env: simpy.Environment, storage_system: EdgeStorageSystem,
                 failure_rate: float, config: Dict, repair_algorithm: BaseRepairAlgorithm,
                 log_callback=None, stop_event=None):
        self.env = env
        self.storage = storage_system
        self.failure_rate = failure_rate
        self.config = config
        self.repair_algorithm = repair_algorithm
        self.log_callback = log_callback
        self.active = True
        self.stop_event = stop_event
        
    def run(self):
        while self.active and (self.stop_event is None or not self.stop_event.is_set()):
            if self.failure_rate > 0:
                time_to_failure = random.expovariate(self.failure_rate)
                yield self.env.timeout(time_to_failure)
                
                if self.active and (self.stop_event is None or not self.stop_event.is_set()):
                    if self.storage.num_nodes > 0:
                        node_idx = random.randint(0, self.storage.num_nodes - 1)
                        node = self.storage.nodes[node_idx]
                        
                        if node.status == NodeStatus.ONLINE:
                            node.fail()
                            self._log(f"[FAILURE] Node {node.node_id} failed at time {self.env.now:.1f}")
                            self.env.process(self._schedule_recovery(node))
    
    def _schedule_recovery(self, node: EdgeNode):
        detection_delay = random.uniform(
            self.config.get('failure_detection_delay', 1.0),
            self.config.get('failure_detection_delay', 1.0) * 2
        )
        yield self.env.timeout(detection_delay)
        
        if self.active and node.status == NodeStatus.OFFLINE:
            self._log(f"[RECOVERY] Node {node.node_id} starting recovery")
            yield self.env.process(node.recover())
            self._log(f"[RECOVERY] Node {node.node_id} recovered at time {self.env.now:.1f}")
            self.env.process(self._run_repair_algorithm(node.node_id))
    
    def _run_repair_algorithm(self, failed_node_id: int):
        self._log(f"[ALGO] Starting algorithm: {self.repair_algorithm.name}")
        # Используем process для правильной работы с генераторами
        yield self.env.process(self.repair_algorithm.repair(
            self.env, self.storage, failed_node_id, self._log
        ))
        self._log(f"[ALGO] Algorithm {self.repair_algorithm.name} completed")
    
    def _log(self, message: str):
        if self.log_callback:
            self.log_callback(f"[{self.env.now:.1f}] {message}")

class MetricsCollector:
    def __init__(self):
        self.reset()
    
    def reset(self):
        self.node_failures = []
        self.node_recoveries = []
        self.write_successes = []
        self.write_failures = []
        self.read_successes = []
        self.read_failures = []
        self.repair_successes = []
        self.availability_history = []
        self.health_history = []
        
        self.simulation_start_time = None
        self.simulation_end_time = None
        self.simulation_params = {}
        
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
    
    def set_simulation_params(self, params: Dict):
        self.simulation_params = params
        self.simulation_start_time = datetime.now()
    
    def set_simulation_end_time(self):
        self.simulation_end_time = datetime.now()
        
    def get_summary(self) -> Dict:
        return {
            'total_failures': len(self.node_failures),
            'total_recoveries': len(self.node_recoveries),
            'total_writes_success': len(self.write_successes),
            'total_writes_failed': len(self.write_failures),
            'total_reads_success': len(self.read_successes),
            'total_reads_failed': len(self.read_failures),
            'total_repairs': len(self.repair_successes),
            'avg_replicas_per_write': np.mean([w[1] for w in self.write_successes]) if self.write_successes else 0,
            'write_success_rate': len(self.write_successes) / (len(self.write_successes) + len(self.write_failures)) if (self.write_successes or self.write_failures) else 1.0,
            'avg_availability': np.mean([a[1] for a in self.availability_history]) if self.availability_history else 1.0,
            'min_availability': min([a[1] for a in self.availability_history]) if self.availability_history else 1.0,
            'max_availability': max([a[1] for a in self.availability_history]) if self.availability_history else 1.0,
            'total_blocks_written': len(self.write_successes)
        }
    
    def _open_file_location(self, filepath: str):
        try:
            path = os.path.dirname(filepath)
            if not path:
                path = os.getcwd()
            
            if os.path.exists(path):
                if platform.system() == 'Windows':
                    os.startfile(path)
                elif platform.system() == 'Darwin':
                    subprocess.run(['open', path], check=False)
                else:
                    subprocess.run(['xdg-open', path], check=False)
        except Exception as e:
            print(f"Could not open folder: {e}")
    
    def export_to_csv(self, filepath: str) -> Tuple[bool, str]:
        try:
            directory = os.path.dirname(filepath)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
            
            if os.path.exists(filepath):
                if not os.access(filepath, os.W_OK):
                    return False, f"No write permission: {filepath}"
            
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                writer.writerow(['#' * 80])
                writer.writerow(['# ADAPTIVE EDGE STORAGE SIMULATOR - RESULTS'])
                writer.writerow(['#' * 80])
                writer.writerow([])
                
                writer.writerow(['[SIMULATION PARAMETERS]'])
                writer.writerow(['Parameter', 'Value'])
                for key, value in self.simulation_params.items():
                    writer.writerow([key, value])
                writer.writerow(['Start time', self.simulation_start_time])
                writer.writerow(['End time', self.simulation_end_time])
                writer.writerow([])
                
                summary = self.get_summary()
                writer.writerow(['[SUMMARY STATISTICS]'])
                writer.writerow(['Metric', 'Value'])
                for key, value in summary.items():
                    writer.writerow([key, value])
                writer.writerow([])
                
                writer.writerow(['[AVAILABILITY OVER TIME]'])
                writer.writerow(['Time', 'Availability'])
                for time, score in self.availability_history:
                    writer.writerow([f'{time:.2f}', f'{score:.4f}'])
                writer.writerow([])
                
                writer.writerow(['[NODE HEALTH]'])
                writer.writerow(['Time', 'Online nodes', 'Total nodes'])
                for time, online, total in self.health_history:
                    writer.writerow([f'{time:.2f}', online, total])
                writer.writerow([])
                
                writer.writerow(['[NODE FAILURES]'])
                writer.writerow(['Time', 'Node ID'])
                for time, node_id in self.node_failures:
                    writer.writerow([f'{time:.2f}', node_id])
                writer.writerow([])
                
                writer.writerow(['[WRITE OPERATIONS]'])
                writer.writerow(['Block ID', 'Replicas written', 'Target replicas'])
                for block_id, written, target in self.write_successes:
                    writer.writerow([block_id, written, target])
                writer.writerow([])
                
                writer.writerow(['[REPAIR OPERATIONS]'])
                writer.writerow(['Block ID', 'Source node', 'Target node'])
                for block_id, source, target in self.repair_successes:
                    writer.writerow([block_id, source, target])
            
            self._open_file_location(filepath)
            return True, ""
            
        except PermissionError as e:
            return False, f"Permission error: {e}"
        except OSError as e:
            return False, f"File system error: {e}"
        except Exception as e:
            return False, f"Unknown error: {e}"
    
    def export_to_excel(self, filepath: str) -> Tuple[bool, str]:
        """
        Export to Excel file with formatting and charts
        Returns: (success, error_message)
        """
        if not OPENPYXL_AVAILABLE:
            return False, "openpyxl not installed. Run: pip install openpyxl"
        
        try:
            # Check write permissions
            directory = os.path.dirname(filepath)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
            
            if os.path.exists(filepath):
                if not os.access(filepath, os.W_OK):
                    return False, f"No write permission: {filepath}"
            
            wb = Workbook()
            
            # Styles
            header_font = Font(bold=True, color="FFFFFF")
            header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
            title_font = Font(bold=True, size=14)
            
            # ===== Sheet 1: Parameters and Summary =====
            ws_summary = wb.active
            ws_summary.title = "Parameters"
            
            ws_summary['A1'] = "ADAPTIVE EDGE STORAGE SIMULATOR"
            ws_summary['A1'].font = title_font
            ws_summary.merge_cells('A1:C1')
            
            ws_summary['A3'] = "SIMULATION PARAMETERS"
            ws_summary['A3'].font = header_font
            ws_summary['A3'].fill = header_fill
            
            row = 4
            for key, value in self.simulation_params.items():
                ws_summary[f'A{row}'] = str(key)
                ws_summary[f'B{row}'] = str(value)
                row += 1
            ws_summary[f'A{row}'] = "Start time"
            ws_summary[f'B{row}'] = str(self.simulation_start_time)
            row += 1
            ws_summary[f'A{row}'] = "End time"
            ws_summary[f'B{row}'] = str(self.simulation_end_time)
            row += 2
            
            ws_summary[f'A{row}'] = "SUMMARY STATISTICS"
            ws_summary[f'A{row}'].font = header_font
            ws_summary[f'A{row}'].fill = header_fill
            row += 1
            
            summary = self.get_summary()
            for key, value in summary.items():
                ws_summary[f'A{row}'] = str(key)
                ws_summary[f'B{row}'] = value
                row += 1
            
            # ===== Sheet 2: Availability Data =====
            ws_avail = wb.create_sheet("Availability")
            ws_avail['A1'] = "Time"
            ws_avail['B1'] = "Availability"
            ws_avail['A1'].font = header_font
            ws_avail['B1'].font = header_font
            ws_avail['A1'].fill = header_fill
            ws_avail['B1'].fill = header_fill
            
            for i, (time_val, score) in enumerate(self.availability_history, start=2):
                ws_avail[f'A{i}'] = time_val
                ws_avail[f'B{i}'] = score
            
            # Add chart
            if len(self.availability_history) > 1:
                chart = LineChart()
                chart.title = "Availability over time"
                chart.x_axis.title = "Time"
                chart.y_axis.title = "Availability"
                
                data = Reference(ws_avail, min_col=2, min_row=1, max_row=len(self.availability_history)+1)
                categories = Reference(ws_avail, min_col=1, min_row=2, max_row=len(self.availability_history)+1)
                chart.add_data(data, titles_from_data=True)
                chart.set_categories(categories)
                ws_avail.add_chart(chart, "D2")
            
            # ===== Sheet 3: Node Health =====
            ws_health = wb.create_sheet("NodeHealth")
            ws_health['A1'] = "Time"
            ws_health['B1'] = "Online nodes"
            ws_health['C1'] = "Total nodes"
            for col in ['A', 'B', 'C']:
                ws_health[f'{col}1'].font = header_font
                ws_health[f'{col}1'].fill = header_fill
            
            for i, (time_val, online, total) in enumerate(self.health_history, start=2):
                ws_health[f'A{i}'] = time_val
                ws_health[f'B{i}'] = online
                ws_health[f'C{i}'] = total
            
            # ===== Sheet 4: Failures =====
            ws_failures = wb.create_sheet("Failures")
            ws_failures['A1'] = "Time"
            ws_failures['B1'] = "Node ID"
            ws_failures['A1'].font = header_font
            ws_failures['B1'].font = header_font
            ws_failures['A1'].fill = header_fill
            ws_failures['B1'].fill = header_fill
            
            for i, (time_val, node_id) in enumerate(self.node_failures, start=2):
                ws_failures[f'A{i}'] = time_val
                ws_failures[f'B{i}'] = node_id
            
            # ===== Sheet 5: Operations =====
            ws_ops = wb.create_sheet("Operations")
            ws_ops['A1'] = "Operation"
            ws_ops['B1'] = "Block ID"
            ws_ops['C1'] = "Info"
            for col in ['A', 'B', 'C']:
                ws_ops[f'{col}1'].font = header_font
                ws_ops[f'{col}1'].fill = header_fill
            
            row = 2
            for block_id, written, target in self.write_successes:
                ws_ops[f'A{row}'] = "Write OK"
                ws_ops[f'B{row}'] = block_id
                ws_ops[f'C{row}'] = f"{written}/{target} replicas"
                row += 1
            
            for block_id, reason in self.write_failures:
                ws_ops[f'A{row}'] = "Write FAIL"
                ws_ops[f'B{row}'] = block_id
                ws_ops[f'C{row}'] = reason
                row += 1
            
            for block_id, source, target in self.repair_successes:
                ws_ops[f'A{row}'] = "Repair"
                ws_ops[f'B{row}'] = block_id
                ws_ops[f'C{row}'] = f"{source} -> {target}"
                row += 1
            
            # ===== Auto-fit column widths (FIXED - safe approach) =====
            from openpyxl.utils import get_column_letter
            
            for ws in wb.worksheets:
                if ws.max_row > 0 and ws.max_column > 0:
                    for col_idx in range(1, ws.max_column + 1):
                        max_length = 0
                        
                        # Get column letter safely
                        try:
                            col_letter = get_column_letter(col_idx)
                        except Exception:
                            continue
                        
                        # Find max length in this column
                        for row_idx in range(1, min(ws.max_row + 1, 5000)):  # Limit rows
                            try:
                                cell = ws.cell(row=row_idx, column=col_idx)
                                # Skip merged cells - they don't have column_letter attribute
                                if hasattr(cell, 'value') and cell.value is not None:
                                    # Check if this cell is part of a merged range
                                    is_merged = False
                                    if hasattr(ws, 'merged_cells'):
                                        for merged_range in ws.merged_cells.ranges:
                                            if cell.coordinate in merged_range:
                                                is_merged = True
                                                break
                                    
                                    if not is_merged:
                                        cell_length = len(str(cell.value))
                                        if cell_length > max_length:
                                            max_length = cell_length
                            except Exception:
                                continue
                        
                        # Set width with padding
                        adjusted_width = min(max_length + 2, 50)
                        if adjusted_width > 5:  # Only set if meaningful
                            try:
                                ws.column_dimensions[col_letter].width = adjusted_width
                            except Exception:
                                pass
            
            wb.save(filepath)
            self._open_file_location(filepath)
            return True, ""
            
        except PermissionError as e:
            return False, f"Permission error: {e}. File may be open in Excel."
        except OSError as e:
            return False, f"File system error: {e}"
        except Exception as e:
            return False, f"Unknown error: {e}"