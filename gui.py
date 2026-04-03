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
    """Главное окно программы с графическим интерфейсом"""
    
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Adaptive Edge Storage Simulator - AESS v1.1")
        self.root.geometry("1280x800")
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        
        # Переменные для параметров симуляции
        self.num_nodes = tk.IntVar(value=8)
        self.replication_factor = tk.IntVar(value=3)
        self.failure_rate = tk.DoubleVar(value=0.5)
        self.simulation_time = tk.IntVar(value=100)
        self.min_recovery_time = tk.DoubleVar(value=2.0)
        self.max_recovery_time = tk.DoubleVar(value=8.0)
        
        # Состояние симуляции
        self.is_running = False
        self.simulation_thread = None
        self.sim_env = None
        self.storage_system = None
        self.metrics = MetricsCollector()
        
        # Построение интерфейса
        self._build_ui()
        
    def _build_ui(self):
        """Построение пользовательского интерфейса"""
        
        # Основной контейнер с панелями
        main_paned = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_paned.pack(fill=tk.BOTH, expand=True)
        
        # ===== ЛЕВАЯ ПАНЕЛЬ - Управление =====
        left_frame = ttk.Frame(main_paned, width=300)
        main_paned.add(left_frame, weight=0)
        
        # Заголовок
        title_label = ttk.Label(left_frame, text="⚙️ Adaptive Edge Storage Simulator", 
                                font=('Arial', 14, 'bold'))
        title_label.pack(pady=10)
        
        # Рамка параметров
        params_frame = ttk.LabelFrame(left_frame, text="Параметры симуляции", padding=10)
        params_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # Количество узлов
        ttk.Label(params_frame, text="Количество узлов:").grid(row=0, column=0, sticky=tk.W, pady=5)
        nodes_scale = ttk.Scale(params_frame, from_=2, to=32, variable=self.num_nodes, orient=tk.HORIZONTAL)
        nodes_scale.grid(row=0, column=1, padx=10, sticky=tk.EW)
        nodes_label = ttk.Label(params_frame, textvariable=self.num_nodes)
        nodes_label.grid(row=0, column=2, padx=5)
        
        # Фактор репликации
        ttk.Label(params_frame, text="Фактор репликации:").grid(row=1, column=0, sticky=tk.W, pady=5)
        rep_scale = ttk.Scale(params_frame, from_=1, to=5, variable=self.replication_factor, orient=tk.HORIZONTAL)
        rep_scale.grid(row=1, column=1, padx=10, sticky=tk.EW)
        rep_label = ttk.Label(params_frame, textvariable=self.replication_factor)
        rep_label.grid(row=1, column=2, padx=5)
        
        # Интенсивность сбоев
        ttk.Label(params_frame, text="Интенсивность сбоев (отк/ед.вр):").grid(row=2, column=0, sticky=tk.W, pady=5)
        fail_scale = ttk.Scale(params_frame, from_=0.0, to=2.0, variable=self.failure_rate, orient=tk.HORIZONTAL)
        fail_scale.grid(row=2, column=1, padx=10, sticky=tk.EW)
        fail_label = ttk.Label(params_frame, textvariable=self.failure_rate)
        fail_label.grid(row=2, column=2, padx=5)
        
        # Время симуляции
        ttk.Label(params_frame, text="Время симуляции (ед.вр):").grid(row=3, column=0, sticky=tk.W, pady=5)
        time_scale = ttk.Scale(params_frame, from_=10, to=500, variable=self.simulation_time, orient=tk.HORIZONTAL)
        time_scale.grid(row=3, column=1, padx=10, sticky=tk.EW)
        time_label = ttk.Label(params_frame, textvariable=self.simulation_time)
        time_label.grid(row=3, column=2, padx=5)
        
        # Время восстановления
        ttk.Label(params_frame, text="Время восстановления (мин/макс):").grid(row=4, column=0, sticky=tk.W, pady=5)
        rec_frame = ttk.Frame(params_frame)
        rec_frame.grid(row=4, column=1, columnspan=2, sticky=tk.EW)
        ttk.Label(rec_frame, text="от").pack(side=tk.LEFT)
        rec_min = ttk.Entry(rec_frame, textvariable=self.min_recovery_time, width=8)
        rec_min.pack(side=tk.LEFT, padx=2)
        ttk.Label(rec_frame, text="до").pack(side=tk.LEFT)
        rec_max = ttk.Entry(rec_frame, textvariable=self.max_recovery_time, width=8)
        rec_max.pack(side=tk.LEFT, padx=2)
        
        params_frame.columnconfigure(1, weight=1)
        
        # Кнопки управления
        btn_frame = ttk.Frame(left_frame)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)
        
        self.start_btn = ttk.Button(btn_frame, text="▶ Запустить симуляцию", command=self._start_simulation)
        self.start_btn.pack(fill=tk.X, pady=2)
        
        self.stop_btn = ttk.Button(btn_frame, text="⏹ Остановить", command=self._stop_simulation, state=tk.DISABLED)
        self.stop_btn.pack(fill=tk.X, pady=2)
        
        self.reset_btn = ttk.Button(btn_frame, text="🔄 Сбросить метрики", command=self._reset_metrics)
        self.reset_btn.pack(fill=tk.X, pady=2)
        
        # Кнопка экспорта CSV
        self.export_btn = ttk.Button(btn_frame, text="💾 Экспорт в CSV", command=self._export_to_csv)
        self.export_btn.pack(fill=tk.X, pady=2)
        
        # Статистика
        stats_frame = ttk.LabelFrame(left_frame, text="📊 Статистика симуляции", padding=10)
        stats_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        self.stats_text = scrolledtext.ScrolledText(stats_frame, height=12, width=35, font=('Courier', 9))
        self.stats_text.pack(fill=tk.BOTH, expand=True)
        
        # ===== ПРАВАЯ ПАНЕЛЬ - Визуализация и Логи =====
        right_frame = ttk.Frame(main_paned)
        main_paned.add(right_frame, weight=1)
        
        # Визуализация графиков
        viz_frame = ttk.LabelFrame(right_frame, text="📈 Визуализация метрик", padding=5)
        viz_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Создание фигуры matplotlib
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
        
        # Прогресс-бар
        self.progress = ttk.Progressbar(right_frame, mode='indeterminate')
        self.progress.pack(fill=tk.X, pady=5)
        
    def _log(self, message: str):
        """Добавление сообщения в лог"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)
        
    def _update_stats_display(self):
        """Обновление отображения статистики"""
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
        """Обновление графиков"""
        self.ax1.clear()
        self.ax2.clear()
        
        # График доступности
        if self.metrics.availability_history:
            times = [t[0] for t in self.metrics.availability_history]
            scores = [t[1] for t in self.metrics.availability_history]
            self.ax1.plot(times, scores, 'b-', linewidth=2)
            self.ax1.set_ylabel('Доступность данных')
            self.ax1.set_xlabel('Время')
            self.ax1.set_ylim(0, 1.1)
            self.ax1.grid(True, alpha=0.3)
            self.ax1.set_title('Доступность данных во времени')
        
        # График здоровья узлов
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
        """Экспорт результатов симуляции в CSV файл"""
        if not self.metrics.availability_history and not self.metrics.node_failures:
            messagebox.showwarning("Нет данных", 
                                  "Сначала запустите симуляцию для получения данных")
            return
        
        # Диалог выбора файла
        filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=f"aess_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        
        if filepath:
            success = self.metrics.export_to_csv(filepath)
            if success:
                self._log(f"📁 Результаты экспортированы в: {filepath}")
                messagebox.showinfo("Экспорт завершён", 
                                   f"Результаты успешно сохранены в файл:\n{filepath}")
            else:
                messagebox.showerror("Ошибка экспорта", 
                                    "Не удалось сохранить файл. Проверьте права доступа.")
        
    def _start_simulation(self):
        """Запуск симуляции в отдельном потоке"""
        if self.is_running:
            return
        
        self.is_running = True
        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.export_btn.config(state=tk.DISABLED)
        self.progress.start()
        
        # Сохраняем параметры симуляции для CSV
        sim_params = {
            'num_nodes': self.num_nodes.get(),
            'replication_factor': self.replication_factor.get(),
            'failure_rate': self.failure_rate.get(),
            'simulation_time': self.simulation_time.get(),
            'min_recovery_time': self.min_recovery_time.get(),
            'max_recovery_time': self.max_recovery_time.get(),
            'algorithm': 'BaseParallelRepair (v1.1)'
        }
        self.metrics.set_simulation_params(sim_params)
        
        self._log("🚀 Запуск симуляции...")
        self._log(f"   Узлов: {self.num_nodes.get()}")
        self._log(f"   Репликация: {self.replication_factor.get()}")
        self._log(f"   Интенсивность сбоев: {self.failure_rate.get()}")
        self._log(f"   Время симуляции: {self.simulation_time.get()}")
        
        # Запуск в отдельном потоке
        self.simulation_thread = threading.Thread(target=self._run_simulation)
        self.simulation_thread.daemon = True
        self.simulation_thread.start()
        
    def _run_simulation(self):
        """Основной цикл симуляции (выполняется в отдельном потоке)"""
        try:
            # Настройка параметров
            config = {
                'min_recovery_time': self.min_recovery_time.get(),
                'max_recovery_time': self.max_recovery_time.get(),
                'min_writes_for_success': 1,
                'failure_detection_delay': 1.0
            }
            
            # Создание среды simpy
            env = simpy.Environment()
            
            # Создание метрик
            self.metrics = MetricsCollector()
            self.metrics.set_simulation_params({
                'num_nodes': self.num_nodes.get(),
                'replication_factor': self.replication_factor.get(),
                'failure_rate': self.failure_rate.get(),
                'simulation_time': self.simulation_time.get(),
                'min_recovery_time': self.min_recovery_time.get(),
                'max_recovery_time': self.max_recovery_time.get(),
                'algorithm': 'BaseParallelRepair (v1.1)'
            })
            
            # Создание системы хранения
            storage = EdgeStorageSystem(
                env=env,
                num_nodes=self.num_nodes.get(),
                replication_factor=self.replication_factor.get(),
                config=config,
                metrics_collector=self.metrics
            )
            
            # Симулятор агрессивной среды
            environment = AggressiveEnvironment(
                env=env,
                storage_system=storage,
                failure_rate=self.failure_rate.get(),
                config=config,
                log_callback=self._log
            )
            
            # Запуск генерации сбоев
            env.process(environment.run())
            
            # Запуск генератора нагрузки (запись/чтение данных)
            def load_generator():
                while True:
                    # Запись новых блоков
                    if random.random() < 0.3:  # 30% шанс записи
                        storage.write_block()
                    # Чтение существующих блоков
                    elif storage.block_placement and random.random() < 0.5:
                        block_id = random.choice(list(storage.block_placement.keys()))
                        storage.read_block(block_id)
                    yield env.timeout(random.uniform(0.5, 1.5))
            
            env.process(load_generator())
            
            # Мониторинг метрик
            def metrics_monitor():
                while True:
                    # Запись метрик каждые 2 единицы времени
                    availability = storage.get_availability_score()
                    online_nodes = sum(1 for n in storage.nodes if n.status == NodeStatus.ONLINE)
                    total_nodes = storage.num_nodes
                    
                    self.metrics.record_availability(env.now, availability)
                    self.metrics.record_health(env.now, online_nodes, total_nodes)
                    
                    # Обновление UI (используем after)
                    self.root.after(0, self._update_stats_display)
                    self.root.after(0, self._update_plots)
                    
                    yield env.timeout(2.0)
            
            env.process(metrics_monitor())
            
            # Запуск симуляции
            env.run(until=self.simulation_time.get())
            
            # Фиксируем окончание симуляции
            self.metrics.set_simulation_end_time()
            
            self._log("✅ Симуляция завершена успешно")
            
        except Exception as e:
            self._log(f"❌ Ошибка в симуляции: {e}")
            import traceback
            self._log(traceback.format_exc())
        finally:
            self.root.after(0, self._simulation_finished)
    
    def _simulation_finished(self):
        """Обработка завершения симуляции"""
        self.is_running = False
        self.start_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.export_btn.config(state=tk.NORMAL)
        self.progress.stop()
        self._update_stats_display()
        self._update_plots()
        self._log("🏁 Симуляция остановлена")
        
    def _stop_simulation(self):
        """Остановка симуляции"""
        if self.is_running:
            self.is_running = False
            self._log("⏸ Остановка симуляции по запросу...")
        
    def _reset_metrics(self):
        """Сброс метрик и очистка графиков"""
        self.metrics = MetricsCollector()
        self._update_stats_display()
        
        # Очистка графиков
        self.ax1.clear()
        self.ax2.clear()
        self.ax1.set_title('Доступность данных во времени')
        self.ax2.set_title('Состояние узлов во времени')
        self.canvas.draw()
        
        self._log("🔄 Метрики сброшены")
        
    def _on_closing(self):
        """Обработка закрытия окна"""
        if self.is_running:
            self.is_running = False
            time.sleep(0.5)
        self.root.destroy()
        
    def run(self):
        """Запуск GUI приложения"""
        self.root.mainloop()
