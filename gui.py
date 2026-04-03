import random
import sys
import threading
import time
from tkinter import filedialog
import simpy
from alg import AdaptiveParallelRepairAlgorithm, ParallelRepairAlgorithm, PriorityBasedRepairAlgorithm, SequentialRepairAlgorithm
from cfg import AggressiveEnvironment, EdgeStorageSystem, MetricsCollector

from datetime import datetime
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

from models import NodeStatus

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
    import tkinter as tk
    from tkinter import ttk, messagebox
    from tkinter import scrolledtext
except ImportError as e:
    print(f"Ошибка импорта tkinter: {e}")
    print("На Linux может потребоваться: sudo apt-get install python3-tk")
    sys.exit(1)

class SimulationGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Adaptive Edge Storage Simulator - AESS v4.2")
        self.root.geometry("1320x850")
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        
        self.algorithms = {
            "sequential": SequentialRepairAlgorithm(),
            "parallel": ParallelRepairAlgorithm(),
            "priority": PriorityBasedRepairAlgorithm(),
            "adaptive": AdaptiveParallelRepairAlgorithm()
        }
        self.current_algorithm = tk.StringVar(value="adaptive")
        
        self.num_nodes = tk.IntVar(value=8)
        self.replication_factor = tk.IntVar(value=3)
        self.failure_rate = tk.DoubleVar(value=0.5)
        self.simulation_time = tk.IntVar(value=100)
        self.min_recovery_time = tk.DoubleVar(value=2.0)
        self.max_recovery_time = tk.DoubleVar(value=8.0)
        self.infinite_mode = tk.BooleanVar(value=False)
        
        self.stop_simulation_flag = threading.Event()
        self.is_running = False
        self.simulation_thread = None
        self.metrics = MetricsCollector()
        
        self._build_ui()
    
    def _build_ui(self):
        main_paned = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_paned.pack(fill=tk.BOTH, expand=True)
        
        left_frame = ttk.Frame(main_paned, width=350)
        main_paned.add(left_frame, weight=0)
        
        title_label = ttk.Label(left_frame, text="Adaptive Edge Storage Simulator", 
                                font=('Arial', 14, 'bold'))
        title_label.pack(pady=10)
        
        params_frame = ttk.LabelFrame(left_frame, text="Simulation Parameters", padding=10)
        params_frame.pack(fill=tk.X, padx=10, pady=5)
        
        nodes_frame = ttk.Frame(params_frame)
        nodes_frame.pack(fill=tk.X, pady=5)
        ttk.Label(nodes_frame, text="Number of nodes:").pack(side=tk.LEFT)
        self.nodes_entry = NumericEntry(nodes_frame, "", 2, 32, 8, step=1, is_int=True)
        self.nodes_entry.pack(side=tk.RIGHT)
        
        rep_frame = ttk.Frame(params_frame)
        rep_frame.pack(fill=tk.X, pady=5)
        ttk.Label(rep_frame, text="Replication factor:").pack(side=tk.LEFT)
        self.rep_entry = NumericEntry(rep_frame, "", 1, 5, 3, step=1, is_int=True)
        self.rep_entry.pack(side=tk.RIGHT)
        
        fail_frame = ttk.Frame(params_frame)
        fail_frame.pack(fill=tk.X, pady=5)
        ttk.Label(fail_frame, text="Failure rate:").pack(side=tk.LEFT)
        self.fail_entry = NumericEntry(fail_frame, "", 0, 2, 0.5, step=0.1, is_int=False)
        self.fail_entry.pack(side=tk.RIGHT)
        
        rec_frame = ttk.LabelFrame(params_frame, text="Node recovery time", padding=5)
        rec_frame.pack(fill=tk.X, pady=5)
        
        rec_min_frame = ttk.Frame(rec_frame)
        rec_min_frame.pack(fill=tk.X, pady=2)
        ttk.Label(rec_min_frame, text="Min:").pack(side=tk.LEFT)
        self.rec_min_entry = NumericEntry(rec_min_frame, "", 0.5, 20, 2.0, step=0.5, is_int=False)
        self.rec_min_entry.pack(side=tk.RIGHT)
        
        rec_max_frame = ttk.Frame(rec_frame)
        rec_max_frame.pack(fill=tk.X, pady=2)
        ttk.Label(rec_max_frame, text="Max:").pack(side=tk.LEFT)
        self.rec_max_entry = NumericEntry(rec_max_frame, "", 1, 30, 8.0, step=0.5, is_int=False)
        self.rec_max_entry.pack(side=tk.RIGHT)
        
        algo_frame = ttk.LabelFrame(params_frame, text="Repair Algorithm", padding=5)
        algo_frame.pack(fill=tk.X, pady=5)
        
        for key, algo in self.algorithms.items():
            rb = ttk.Radiobutton(
                algo_frame, 
                text=algo.name, 
                variable=self.current_algorithm, 
                value=key
            )
            rb.pack(anchor=tk.W, pady=2)
            
            desc_label = ttk.Label(algo_frame, text=f"  {algo.description}", 
                                   font=('Arial', 8), foreground="gray")
            desc_label.pack(anchor=tk.W, padx=15, pady=(0, 5))
        
        mode_frame = ttk.LabelFrame(params_frame, text="Mode", padding=5)
        mode_frame.pack(fill=tk.X, pady=5)
        
        self.time_mode_radio = ttk.Radiobutton(mode_frame, text="Timed", 
                                                variable=self.infinite_mode, value=False)
        self.time_mode_radio.pack(anchor=tk.W, pady=2)
        
        self.time_entry = NumericEntry(mode_frame, "Time:", 10, 10000, 100, step=50, is_int=True)
        self.time_entry.pack(anchor=tk.W, padx=20, pady=2)
        
        self.infinite_radio = ttk.Radiobutton(mode_frame, text="Infinite (until Stop)", 
                                               variable=self.infinite_mode, value=True)
        self.infinite_radio.pack(anchor=tk.W, pady=2)
        
        btn_frame = ttk.Frame(left_frame)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)
        
        self.start_btn = ttk.Button(btn_frame, text="Start Simulation", command=self._start_simulation)
        self.start_btn.pack(fill=tk.X, pady=2)
        
        self.stop_btn = ttk.Button(btn_frame, text="Stop", command=self._stop_simulation, state=tk.DISABLED)
        self.stop_btn.pack(fill=tk.X, pady=2)
        
        self.reset_btn = ttk.Button(btn_frame, text="Reset Metrics", command=self._reset_metrics)
        self.reset_btn.pack(fill=tk.X, pady=2)
        
        export_frame = ttk.LabelFrame(btn_frame, text="Export Results", padding=5)
        export_frame.pack(fill=tk.X, pady=5)
        
        self.export_csv_btn = ttk.Button(export_frame, text="Export to CSV", 
                                          command=lambda: self._export_results('csv'))
        self.export_csv_btn.pack(fill=tk.X, pady=2)
        
        self.export_excel_btn = ttk.Button(export_frame, text="Export to Excel", 
                                            command=lambda: self._export_results('excel'))
        self.export_excel_btn.pack(fill=tk.X, pady=2)
        
        if not OPENPYXL_AVAILABLE:
            self.export_excel_btn.config(state=tk.DISABLED)
            ttk.Label(export_frame, text="Install openpyxl for Excel export", 
                      font=('Arial', 8), foreground="red").pack()
        
        stats_frame = ttk.LabelFrame(left_frame, text="Statistics", padding=10)
        stats_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        self.stats_text = scrolledtext.ScrolledText(stats_frame, height=12, width=38, font=('Courier', 9))
        self.stats_text.pack(fill=tk.BOTH, expand=True)
        
        right_frame = ttk.Frame(main_paned)
        main_paned.add(right_frame, weight=1)
        
        viz_frame = ttk.LabelFrame(right_frame, text="Visualization", padding=5)
        viz_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.fig = Figure(figsize=(8, 5), dpi=80)
        self.ax1 = self.fig.add_subplot(211)
        self.ax2 = self.fig.add_subplot(212)
        
        self.canvas = FigureCanvasTkAgg(self.fig, master=viz_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        log_frame = ttk.LabelFrame(right_frame, text="Event Log", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, height=10, font=('Courier', 9))
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        status_frame = ttk.Frame(right_frame)
        status_frame.pack(fill=tk.X, pady=5)
        
        self.progress = ttk.Progressbar(status_frame, mode='indeterminate')
        self.progress.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        self.mode_indicator = ttk.Label(status_frame, text="", font=('Arial', 9))
        self.mode_indicator.pack(side=tk.RIGHT, padx=10)
        
        self.algo_indicator = ttk.Label(status_frame, text="", font=('Arial', 9))
        self.algo_indicator.pack(side=tk.RIGHT, padx=10)
    
    def _log(self, message: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)
    
    def _update_stats_display(self):
        summary = self.metrics.get_summary()
        stats_str = f"""
+--------------------------------------+
|           SUMMARY STATISTICS         |
+--------------------------------------+
| Node failures:           {summary['total_failures']:>6}
| Recoveries:              {summary['total_recoveries']:>6}
| Successful writes:       {summary['total_writes_success']:>6}
| Failed writes:           {summary['total_writes_failed']:>6}
| Successful reads:        {summary['total_reads_success']:>6}
| Failed reads:            {summary['total_reads_failed']:>6}
| Repairs:                 {summary['total_repairs']:>6}
+--------------------------------------+
| Avg replicas per write:  {summary['avg_replicas_per_write']:>6.2f}
| Write success rate:      {summary['write_success_rate']*100:>6.1f}%
| Avg availability:        {summary['avg_availability']*100:>6.1f}%
| Min availability:        {summary['min_availability']*100:>6.1f}%
+--------------------------------------+
"""
        self.stats_text.delete(1.0, tk.END)
        self.stats_text.insert(tk.END, stats_str)
    
    def _update_plots(self):
        self.ax1.clear()
        self.ax2.clear()
        
        if self.metrics.availability_history:
            times = [t[0] for t in self.metrics.availability_history]
            scores = [t[1] for t in self.metrics.availability_history]
            self.ax1.plot(times, scores, 'b-', linewidth=2)
            self.ax1.set_ylabel('Availability')
            self.ax1.set_xlabel('Time')
            self.ax1.set_ylim(0, 1.1)
            self.ax1.grid(True, alpha=0.3)
            self.ax1.set_title('Data Availability over Time')
        
        if self.metrics.health_history:
            times = [t[0] for t in self.metrics.health_history]
            online = [t[1] for t in self.metrics.health_history]
            total = [t[2] for t in self.metrics.health_history]
            self.ax2.plot(times, online, 'g-', linewidth=2, label='Online nodes')
            self.ax2.plot(times, total, 'r--', linewidth=1, label='Total nodes')
            self.ax2.set_ylabel('Node Count')
            self.ax2.set_xlabel('Time')
            self.ax2.legend()
            self.ax2.grid(True, alpha=0.3)
            self.ax2.set_title('Node Health over Time')
        
        self.fig.tight_layout()
        self.canvas.draw()
    
    def _export_results(self, format_type: str):
        if not self.metrics.availability_history and not self.metrics.node_failures:
            messagebox.showwarning("No Data", "Run a simulation first to get data")
            return
        
        if format_type == 'csv':
            filetypes = [("CSV files", "*.csv"), ("All files", "*.*")]
            default_ext = ".csv"
            default_name = f"aess_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            export_func = self.metrics.export_to_csv
        else:
            if not OPENPYXL_AVAILABLE:
                messagebox.showerror("Error", "openpyxl not installed.\nRun: pip install openpyxl")
                return
            filetypes = [("Excel files", "*.xlsx"), ("All files", "*.*")]
            default_ext = ".xlsx"
            default_name = f"aess_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            export_func = self.metrics.export_to_excel
        
        filepath = filedialog.asksaveasfilename(
            defaultextension=default_ext,
            filetypes=filetypes,
            initialfile=default_name
        )
        
        if filepath:
            self._log(f"Exporting to {format_type.upper()}...")
            success, error_msg = export_func(filepath)
            
            if success:
                self._log(f"Export successful: {filepath}")
                messagebox.showinfo("Export Complete", f"Results saved to:\n{filepath}")
            else:
                self._log(f"Export error: {error_msg}")
                messagebox.showerror("Export Error", error_msg)
    
    def _start_simulation(self):
        if self.is_running:
            return
        
        num_nodes = self.nodes_entry.get()
        replication_factor = self.rep_entry.get()
        failure_rate = self.fail_entry.get()
        min_recovery = self.rec_min_entry.get()
        max_recovery = self.rec_max_entry.get()
        algorithm_key = self.current_algorithm.get()
        
        if min_recovery >= max_recovery:
            messagebox.showerror("Error", "Min recovery time must be less than max")
            return
        
        if self.infinite_mode.get():
            simulation_duration = None
            mode_text = "infinite"
        else:
            simulation_duration = self.time_entry.get()
            mode_text = f"{simulation_duration} time units"
        
        self.is_running = True
        self.stop_simulation_flag.clear()
        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.export_csv_btn.config(state=tk.DISABLED)
        self.export_excel_btn.config(state=tk.DISABLED)
        self.progress.start()
        
        algorithm = self.algorithms[algorithm_key]
        self.mode_indicator.config(text=f"Mode: {mode_text}", foreground="blue")
        self.algo_indicator.config(text=f"Algorithm: {algorithm.name}", foreground="green")
        
        sim_params = {
            'num_nodes': num_nodes,
            'replication_factor': replication_factor,
            'failure_rate': failure_rate,
            'simulation_time': simulation_duration if simulation_duration else 'infinite',
            'min_recovery_time': min_recovery,
            'max_recovery_time': max_recovery,
            'algorithm': algorithm.name,
            'algorithm_description': algorithm.description,
            'mode': 'infinite' if simulation_duration is None else 'timed'
        }
        self.metrics.set_simulation_params(sim_params)
        
        self._log("Starting simulation...")
        self._log(f"   Nodes: {num_nodes}")
        self._log(f"   Replication factor: {replication_factor}")
        self._log(f"   Failure rate: {failure_rate}")
        self._log(f"   Algorithm: {algorithm.name}")
        self._log(f"   Mode: {mode_text}")
        
        self.simulation_thread = threading.Thread(
            target=self._run_simulation,
            args=(num_nodes, replication_factor, failure_rate, 
                  min_recovery, max_recovery, simulation_duration, algorithm)
        )
        self.simulation_thread.daemon = True
        self.simulation_thread.start()
    
    def _run_simulation(self, num_nodes, replication_factor, failure_rate,
                        min_recovery, max_recovery, simulation_duration, algorithm):
        try:
            config = {
                'min_recovery_time': min_recovery,
                'max_recovery_time': max_recovery,
                'min_writes_for_success': 1,
                'failure_detection_delay': 1.0
            }
            
            env = simpy.Environment()
            
            self.metrics = MetricsCollector()
            self.metrics.set_simulation_params({
                'num_nodes': num_nodes,
                'replication_factor': replication_factor,
                'failure_rate': failure_rate,
                'simulation_time': simulation_duration if simulation_duration else 'infinite',
                'min_recovery_time': min_recovery,
                'max_recovery_time': max_recovery,
                'algorithm': algorithm.name,
                'algorithm_description': algorithm.description,
                'mode': 'infinite' if simulation_duration is None else 'timed'
            })
            
            storage = EdgeStorageSystem(
                env=env,
                num_nodes=num_nodes,
                replication_factor=replication_factor,
                config=config,
                metrics_collector=self.metrics
            )
            
            environment = AggressiveEnvironment(
                env=env,
                storage_system=storage,
                failure_rate=failure_rate,
                config=config,
                repair_algorithm=algorithm,
                log_callback=self._log,
                stop_event=self.stop_simulation_flag
            )
            
            env.process(environment.run())
            
            def load_generator():
                while not self.stop_simulation_flag.is_set():
                    if random.random() < 0.3:
                        storage.write_block()
                    elif storage.block_placement and random.random() < 0.5:
                        block_id = random.choice(list(storage.block_placement.keys()))
                        storage.read_block(block_id)
                    yield env.timeout(random.uniform(0.5, 1.5))
            
            env.process(load_generator())
            
            def metrics_monitor():
                while not self.stop_simulation_flag.is_set():
                    availability = storage.get_availability_score()
                    online_nodes = sum(1 for n in storage.nodes if n.status == NodeStatus.ONLINE)
                    total_nodes = storage.num_nodes
                    
                    self.metrics.record_availability(env.now, availability)
                    self.metrics.record_health(env.now, online_nodes, total_nodes)
                    
                    self.root.after(0, self._update_stats_display)
                    self.root.after(0, self._update_plots)
                    
                    yield env.timeout(2.0)
            
            env.process(metrics_monitor())
            
            if simulation_duration is None:
                while not self.stop_simulation_flag.is_set():
                    env.run(until=env.now + 10)
                    if self.stop_simulation_flag.is_set():
                        break
            else:
                env.run(until=simulation_duration)
            
            self.metrics.set_simulation_end_time()
            self._log("Simulation completed successfully")
            
        except Exception as e:
            self._log(f"Error: {e}")
            import traceback
            self._log(traceback.format_exc())
        finally:
            self.root.after(0, self._simulation_finished)
    
    def _simulation_finished(self):
        self.is_running = False
        self.start_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.export_csv_btn.config(state=tk.NORMAL)
        if OPENPYXL_AVAILABLE:
            self.export_excel_btn.config(state=tk.NORMAL)
        self.progress.stop()
        self.mode_indicator.config(text="", foreground="black")
        self.algo_indicator.config(text="", foreground="black")
        self._update_stats_display()
        self._update_plots()
        self._log("Simulation stopped")
    
    def _stop_simulation(self):
        if self.is_running:
            self._log("Stopping simulation...")
            self.stop_simulation_flag.set()
    
    def _reset_metrics(self):
        self.metrics = MetricsCollector()
        self._update_stats_display()
        
        self.ax1.clear()
        self.ax2.clear()
        self.ax1.set_title('Data Availability over Time')
        self.ax2.set_title('Node Health over Time')
        self.canvas.draw()
        
        self._log("Metrics reset")
    
    def _on_closing(self):
        if self.is_running:
            self.stop_simulation_flag.set()
            time.sleep(0.5)
        self.root.destroy()
    
    def run(self):
        self.root.mainloop()

class NumericEntry(ttk.Frame):
    def __init__(self, parent, label, from_val, to_val, default, step=1, is_int=True, **kwargs):
        super().__init__(parent, **kwargs)
        self.from_val = from_val
        self.to_val = to_val
        self.step = step
        self.is_int = is_int
        
        if is_int:
            self.value = tk.IntVar(value=default)
        else:
            self.value = tk.DoubleVar(value=default)
        
        if label:
            ttk.Label(self, text=label).pack(side=tk.LEFT, padx=5)
        
        self.minus_btn = ttk.Button(self, text="-", width=3, command=self._decrement)
        self.minus_btn.pack(side=tk.LEFT)
        
        self.entry = ttk.Entry(self, textvariable=self.value, width=8)
        self.entry.pack(side=tk.LEFT, padx=5)
        
        self.plus_btn = ttk.Button(self, text="+", width=3, command=self._increment)
        self.plus_btn.pack(side=tk.LEFT)
        
        self.entry.bind('<FocusOut>', self._validate)
        self.entry.bind('<Return>', self._validate)
    
    def _increment(self):
        current = self.value.get()
        new_val = current + self.step
        if new_val <= self.to_val:
            self.value.set(new_val)
    
    def _decrement(self):
        current = self.value.get()
        new_val = current - self.step
        if new_val >= self.from_val:
            self.value.set(new_val)
    
    def _validate(self, event=None):
        try:
            if self.is_int:
                val = int(self.entry.get())
            else:
                val = float(self.entry.get())
            
            if val < self.from_val:
                val = self.from_val
            elif val > self.to_val:
                val = self.to_val
            
            self.value.set(val)
        except ValueError:
            pass
    
    def get(self):
        return self.value.get()
    
    def set(self, val):
        self.value.set(val)
