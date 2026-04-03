import random
import sys
import threading
import time
from tkinter import filedialog
import simpy
from cfg import AggressiveEnvironment, EdgeStorageSystem, MetricsCollector

from datetime import datetime
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

from models import NodeStatus

try:
    import tkinter as tk
    from tkinter import ttk, messagebox
    from tkinter import scrolledtext
except ImportError as e:
    print(f"Ошибка импорта tkinter: {e}")
    print("На Linux может потребоваться: sudo apt-get install python3-tk")
    sys.exit(1)

class SimulationGUI:
    """Главное окно программы"""
    
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Adaptive Edge Storage Simulator - AESS v2.0")
        self.root.geometry("1280x800")
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        
        # Переменные для параметров симуляции
        self.num_nodes = tk.IntVar(value=8)
        self.replication_factor = tk.IntVar(value=3)
        self.failure_rate = tk.DoubleVar(value=0.5)
        self.simulation_time = tk.IntVar(value=100)
        self.min_recovery_time = tk.DoubleVar(value=2.0)
        self.max_recovery_time = tk.DoubleVar(value=8.0)
        
        # Режим симуляции: False - по времени, True - бесконечная
        self.infinite_mode = tk.BooleanVar(value=False)
        
        # Флаг остановки симуляции
        self.stop_simulation_flag = threading.Event()
        
        # Состояние симуляции
        self.is_running = False
        self.simulation_thread = None
        self.env_thread = None
        self.metrics = MetricsCollector()
        
        # Построение интерфейса
        self._build_ui()
    
    def _validate_int(self, value, from_val, to_val):
        """Валидация целочисленного ввода"""
        try:
            val = int(value)
            if val < from_val:
                return str(from_val)
            if val > to_val:
                return str(to_val)
            return str(val)
        except ValueError:
            return str(from_val)
    
    def _validate_float(self, value, from_val, to_val):
        """Валидация вещественного ввода"""
        try:
            val = float(value)
            if val < from_val:
                return str(from_val)
            if val > to_val:
                return str(to_val)
            return str(val)
        except ValueError:
            return str(from_val)
        
    def _build_ui(self):
        """Построение пользовательского интерфейса"""
        
        main_paned = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_paned.pack(fill=tk.BOTH, expand=True)
        
        # ===== ЛЕВАЯ ПАНЕЛЬ =====
        left_frame = ttk.Frame(main_paned, width=320)
        main_paned.add(left_frame, weight=0)
        
        title_label = ttk.Label(left_frame, text="⚙️ Adaptive Edge Storage Simulator", 
                                font=('Arial', 14, 'bold'))
        title_label.pack(pady=10)
        
        # Рамка параметров
        params_frame = ttk.LabelFrame(left_frame, text="Параметры симуляции", padding=10)
        params_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # Количество узлов (2-32)
        nodes_frame = ttk.Frame(params_frame)
        nodes_frame.pack(fill=tk.X, pady=5)
        ttk.Label(nodes_frame, text="Количество узлов:").pack(side=tk.LEFT)
        self.nodes_entry = NumericEntry(nodes_frame, "", 2, 32, 8, step=1, is_int=True)
        self.nodes_entry.pack(side=tk.RIGHT)
        
        # Фактор репликации (1-5)
        rep_frame = ttk.Frame(params_frame)
        rep_frame.pack(fill=tk.X, pady=5)
        ttk.Label(rep_frame, text="Фактор репликации:").pack(side=tk.LEFT)
        self.rep_entry = NumericEntry(rep_frame, "", 1, 5, 3, step=1, is_int=True)
        self.rep_entry.pack(side=tk.RIGHT)
        
        # Интенсивность сбоев (0-2)
        fail_frame = ttk.Frame(params_frame)
        fail_frame.pack(fill=tk.X, pady=5)
        ttk.Label(fail_frame, text="Интенсивность сбоев:").pack(side=tk.LEFT)
        self.fail_entry = NumericEntry(fail_frame, "", 0, 2, 0.5, step=0.1, is_int=False)
        self.fail_entry.pack(side=tk.RIGHT)
        
        # Время восстановления (мин)
        rec_frame = ttk.LabelFrame(params_frame, text="Время восстановления узла", padding=5)
        rec_frame.pack(fill=tk.X, pady=5)
        
        rec_min_frame = ttk.Frame(rec_frame)
        rec_min_frame.pack(fill=tk.X, pady=2)
        ttk.Label(rec_min_frame, text="Минимум:").pack(side=tk.LEFT)
        self.rec_min_entry = NumericEntry(rec_min_frame, "", 0.5, 20, 2.0, step=0.5, is_int=False)
        self.rec_min_entry.pack(side=tk.RIGHT)
        
        rec_max_frame = ttk.Frame(rec_frame)
        rec_max_frame.pack(fill=tk.X, pady=2)
        ttk.Label(rec_max_frame, text="Максимум:").pack(side=tk.LEFT)
        self.rec_max_entry = NumericEntry(rec_max_frame, "", 1, 30, 8.0, step=0.5, is_int=False)
        self.rec_max_entry.pack(side=tk.RIGHT)
        
        # Режим симуляции
        mode_frame = ttk.LabelFrame(params_frame, text="Режим работы", padding=5)
        mode_frame.pack(fill=tk.X, pady=5)
        
        self.time_mode_radio = ttk.Radiobutton(mode_frame, text="По времени (укажите ниже)", 
                                                variable=self.infinite_mode, value=False)
        self.time_mode_radio.pack(anchor=tk.W, pady=2)
        
        self.time_entry = NumericEntry(mode_frame, "Время симуляции:", 10, 10000, 100, step=50, is_int=True)
        self.time_entry.pack(anchor=tk.W, padx=20, pady=2)
        
        self.infinite_radio = ttk.Radiobutton(mode_frame, text="Бесконечно (до нажатия Стоп)", 
                                               variable=self.infinite_mode, value=True)
        self.infinite_radio.pack(anchor=tk.W, pady=2)
        
        # Кнопки управления
        btn_frame = ttk.Frame(left_frame)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)
        
        self.start_btn = ttk.Button(btn_frame, text="▶ Запустить симуляцию", command=self._start_simulation)
        self.start_btn.pack(fill=tk.X, pady=2)
        
        self.stop_btn = ttk.Button(btn_frame, text="⏹ Остановить", command=self._stop_simulation, state=tk.DISABLED)
        self.stop_btn.pack(fill=tk.X, pady=2)
        
        self.reset_btn = ttk.Button(btn_frame, text="🔄 Сбросить метрики", command=self._reset_metrics)
        self.reset_btn.pack(fill=tk.X, pady=2)
        
        self.export_btn = ttk.Button(btn_frame, text="💾 Экспорт в CSV", command=self._export_to_csv)
        self.export_btn.pack(fill=tk.X, pady=2)
        
        # Статистика
        stats_frame = ttk.LabelFrame(left_frame, text="📊 Статистика симуляции", padding=10)
        stats_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        self.stats_text = scrolledtext.ScrolledText(stats_frame, height=12, width=35, font=('Courier', 9))
        self.stats_text.pack(fill=tk.BOTH, expand=True)
        
        # ===== ПРАВАЯ ПАНЕЛЬ =====
        right_frame = ttk.Frame(main_paned)
        main_paned.add(right_frame, weight=1)
        
        # Визуализация
        viz_frame = ttk.LabelFrame(right_frame, text="📈 Визуализация метрик", padding=5)
        viz_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.fig = Figure(figsize=(8, 5), dpi=80)
        self.ax1 = self.fig.add_subplot(211)
        self.ax2 = self.fig.add_subplot(212)
        
        self.canvas = FigureCanvasTkAgg(self.fig, master=viz_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # Лог событий
        log_frame = ttk.LabelFrame(right_frame, text="📋 Лог событий симуляции", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, height=10, font=('Courier', 9))
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # Прогресс-бар и индикатор режима
        status_frame = ttk.Frame(right_frame)
        status_frame.pack(fill=tk.X, pady=5)
        
        self.progress = ttk.Progressbar(status_frame, mode='indeterminate')
        self.progress.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        self.mode_indicator = ttk.Label(status_frame, text="", font=('Arial', 9))
        self.mode_indicator.pack(side=tk.RIGHT, padx=10)
        
    def _log(self, message: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)
        
    def _update_stats_display(self):
        summary = self.metrics.get_summary()
        stats_str = f"""
╔══════════════════════════════════════╗
║        СВОДНАЯ СТАТИСТИКА            ║
╠══════════════════════════════════════╣
║ Отказы узлов:           {summary['total_failures']:>6}
║ Восстановления:         {summary['total_recoveries']:>6}
║ Успешных записей:       {summary['total_writes_success']:>6}
║ Неудачных записей:      {summary['total_writes_failed']:>6}
║ Успешных чтений:        {summary['total_reads_success']:>6}
║ Неудачных чтений:       {summary['total_reads_failed']:>6}
║ Выполненных ремонтов:   {summary['total_repairs']:>6}
╠══════════════════════════════════════╣
║ Ср. реплик на запись:   {summary['avg_replicas_per_write']:>6.2f}
║ Успешность записи:      {summary['write_success_rate']*100:>6.1f}%
║ Средняя доступность:    {summary['avg_availability']*100:>6.1f}%
║ Мин. доступность:       {summary['min_availability']*100:>6.1f}%
╚══════════════════════════════════════╝
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
            self.ax1.set_ylabel('Доступность данных')
            self.ax1.set_xlabel('Время')
            self.ax1.set_ylim(0, 1.1)
            self.ax1.grid(True, alpha=0.3)
            self.ax1.set_title('Доступность данных во времени')
        
        if self.metrics.health_history:
            times = [t[0] for t in self.metrics.health_history]
            online = [t[1] for t in self.metrics.health_history]
            total = [t[2] for t in self.metrics.health_history]
            self.ax2.plot(times, online, 'g-', linewidth=2, label='Работающие узлы')
            self.ax2.plot(times, total, 'r--', linewidth=1, label='Всего узлов')
            self.ax2.set_ylabel('Количество узлов')
            self.ax2.set_xlabel('Время')
            self.ax2.legend()
            self.ax2.grid(True, alpha=0.3)
            self.ax2.set_title('Состояние узлов во времени')
        
        self.fig.tight_layout()
        self.canvas.draw()
    
    def _export_to_csv(self):
        if not self.metrics.availability_history and not self.metrics.node_failures:
            messagebox.showwarning("Нет данных", "Сначала запустите симуляцию для получения данных")
            return
        
        filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=f"aess_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        
        if filepath:
            success = self.metrics.export_to_csv(filepath)
            if success:
                self._log(f"📁 Результаты экспортированы в: {filepath}")
                messagebox.showinfo("Экспорт завершён", f"Результаты сохранены в:\n{filepath}")
            else:
                messagebox.showerror("Ошибка экспорта", "Не удалось сохранить файл")
    
    def _start_simulation(self):
        if self.is_running:
            return
        
        # Получаем значения из полей ввода
        num_nodes = self.nodes_entry.get()
        replication_factor = self.rep_entry.get()
        failure_rate = self.fail_entry.get()
        min_recovery = self.rec_min_entry.get()
        max_recovery = self.rec_max_entry.get()
        
        # Проверка корректности min/max
        if min_recovery >= max_recovery:
            messagebox.showerror("Ошибка", "Минимальное время восстановления должно быть меньше максимального")
            return
        
        # Определяем время симуляции
        if self.infinite_mode.get():
            simulation_duration = None  # Бесконечно
            mode_text = "бесконечно"
        else:
            simulation_duration = self.time_entry.get()
            mode_text = f"{simulation_duration} ед.вр"
        
        self.is_running = True
        self.stop_simulation_flag.clear()
        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.export_btn.config(state=tk.DISABLED)
        self.progress.start()
        
        # Обновляем индикатор режима
        self.mode_indicator.config(text=f"🔄 Режим: {mode_text}", foreground="blue")
        
        # Сохраняем параметры
        sim_params = {
            'num_nodes': num_nodes,
            'replication_factor': replication_factor,
            'failure_rate': failure_rate,
            'simulation_time': simulation_duration if simulation_duration else 'infinite',
            'min_recovery_time': min_recovery,
            'max_recovery_time': max_recovery,
            'algorithm': 'BaseParallelRepair (v2.0)',
            'mode': 'infinite' if simulation_duration is None else 'timed'
        }
        self.metrics.set_simulation_params(sim_params)
        
        self._log("🚀 Запуск симуляции...")
        self._log(f"   Узлов: {num_nodes}")
        self._log(f"   Репликация: {replication_factor}")
        self._log(f"   Интенсивность сбоев: {failure_rate}")
        self._log(f"   Режим: {mode_text}")
        
        # Запуск в отдельном потоке
        self.simulation_thread = threading.Thread(
            target=self._run_simulation,
            args=(num_nodes, replication_factor, failure_rate, 
                  min_recovery, max_recovery, simulation_duration)
        )
        self.simulation_thread.daemon = True
        self.simulation_thread.start()
    
    def _run_simulation(self, num_nodes, replication_factor, failure_rate,
                        min_recovery, max_recovery, simulation_duration):
        """Основной цикл симуляции"""
        try:
            config = {
                'min_recovery_time': min_recovery,
                'max_recovery_time': max_recovery,
                'min_writes_for_success': 1,
                'failure_detection_delay': 1.0
            }
            
            # Создание среды simpy
            env = simpy.Environment()
            
            # Создание метрик
            self.metrics = MetricsCollector()
            self.metrics.set_simulation_params({
                'num_nodes': num_nodes,
                'replication_factor': replication_factor,
                'failure_rate': failure_rate,
                'simulation_time': simulation_duration if simulation_duration else 'infinite',
                'min_recovery_time': min_recovery,
                'max_recovery_time': max_recovery,
                'algorithm': 'BaseParallelRepair (v2.0)',
                'mode': 'infinite' if simulation_duration is None else 'timed'
            })
            
            # Создание системы хранения
            storage = EdgeStorageSystem(
                env=env,
                num_nodes=num_nodes,
                replication_factor=replication_factor,
                config=config,
                metrics_collector=self.metrics
            )
            
            # Симулятор агрессивной среды
            environment = AggressiveEnvironment(
                env=env,
                storage_system=storage,
                failure_rate=failure_rate,
                config=config,
                log_callback=self._log,
                stop_event=self.stop_simulation_flag
            )
            
            # Запуск генерации сбоев
            env.process(environment.run())
            
            # Генератор нагрузки
            def load_generator():
                while not self.stop_simulation_flag.is_set():
                    if random.random() < 0.3:
                        storage.write_block()
                    elif storage.block_placement and random.random() < 0.5:
                        block_id = random.choice(list(storage.block_placement.keys()))
                        storage.read_block(block_id)
                    yield env.timeout(random.uniform(0.5, 1.5))
            
            env.process(load_generator())
            
            # Мониторинг метрик
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
            
            # Запуск симуляции
            if simulation_duration is None:
                # Бесконечный режим - работаем до флага остановки
                while not self.stop_simulation_flag.is_set():
                    env.run(until=env.now + 10)  # Шагами по 10 единиц
                    if self.stop_simulation_flag.is_set():
                        break
            else:
                # Режим по времени
                env.run(until=simulation_duration)
            
            self.metrics.set_simulation_end_time()
            self._log("✅ Симуляция завершена")
            
        except Exception as e:
            self._log(f"❌ Ошибка: {e}")
            import traceback
            self._log(traceback.format_exc())
        finally:
            self.root.after(0, self._simulation_finished)
    
    def _simulation_finished(self):
        self.is_running = False
        self.start_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.export_btn.config(state=tk.NORMAL)
        self.progress.stop()
        self.mode_indicator.config(text="", foreground="black")
        self._update_stats_display()
        self._update_plots()
        self._log("🏁 Симуляция остановлена")
        
    def _stop_simulation(self):
        if self.is_running:
            self._log("⏸ Остановка симуляции по запросу...")
            self.stop_simulation_flag.set()
        
    def _reset_metrics(self):
        self.metrics = MetricsCollector()
        self._update_stats_display()
        
        self.ax1.clear()
        self.ax2.clear()
        self.ax1.set_title('Доступность данных во времени')
        self.ax2.set_title('Состояние узлов во времени')
        self.canvas.draw()
        
        self._log("🔄 Метрики сброшены")
        
    def _on_closing(self):
        if self.is_running:
            self.stop_simulation_flag.set()
            time.sleep(0.5)
        self.root.destroy()
        
    def run(self):
        self.root.mainloop()

class NumericEntry(ttk.Frame):
    """Поле ввода с меткой, кнопками +/-, и валидацией"""
    
    def __init__(self, parent, label, from_val, to_val, default, step=1, is_int=True, **kwargs):
        super().__init__(parent, **kwargs)
        self.from_val = from_val
        self.to_val = to_val
        self.step = step
        self.is_int = is_int
        
        # Переменная для хранения значения
        if is_int:
            self.value = tk.IntVar(value=default)
        else:
            self.value = tk.DoubleVar(value=default)
        
        # Метка
        ttk.Label(self, text=label).pack(side=tk.LEFT, padx=5)
        
        # Кнопка "-"
        self.minus_btn = ttk.Button(self, text="-", width=3, command=self._decrement)
        self.minus_btn.pack(side=tk.LEFT)
        
        # Поле ввода
        self.entry = ttk.Entry(self, textvariable=self.value, width=8)
        self.entry.pack(side=tk.LEFT, padx=5)
        
        # Кнопка "+"
        self.plus_btn = ttk.Button(self, text="+", width=3, command=self._increment)
        self.plus_btn.pack(side=tk.LEFT)
        
        # Привязываем валидацию
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
            # Если введено не число, возвращаем предыдущее значение
            pass
    
    def get(self):
        return self.value.get()
    
    def set(self, val):
        self.value.set(val)
