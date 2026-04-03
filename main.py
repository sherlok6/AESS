from gui import SimulationGUI

try:
    import simpy
    import numpy as np
    import matplotlib
    matplotlib.use('TkAgg')  # Для корректной работы в GUI
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    from matplotlib.figure import Figure
    import matplotlib.pyplot as plt
    from matplotlib.animation import FuncAnimation
except ImportError as e:
    print(f"Ошибка импорта: {e}")
    print("Установите необходимые библиотеки:")
    print("pip install simpy numpy matplotlib")
    sys.exit(1)

def main():
    """Главная функция запуска программы"""
    print("""
    ╔══════════════════════════════════════════════════════════════════╗
    ║     Adaptive Edge Storage Simulator (AESS) v1.0                  ║
    ║     Программный комплекс для диссертации                        ║
    ║                                                                  ║
    ║     Разработка: параллельные алгоритмы для периферийных СХД     ║
    ║              в агрессивных условиях внешней среды               ║
    ╚══════════════════════════════════════════════════════════════════╝
    """)
    
    app = SimulationGUI()
    app.run()


if __name__ == "__main__":
    main()