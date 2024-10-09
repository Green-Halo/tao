from EventManager.Models.RunnerEvents import RunnerEvents
from EventManager.EventSubscriptionController import EventSubscriptionController
from ConfigValidator.Config.Models.RunTableModel import RunTableModel
from ConfigValidator.Config.Models.FactorModel import FactorModel
from ConfigValidator.Config.Models.RunnerContext import RunnerContext
from ConfigValidator.Config.Models.OperationType import OperationType
from ExtendedTyping.Typing import SupportsStr
from ProgressManager.Output.OutputProcedure import OutputProcedure as output
import psutil
import subprocess
from typing import Dict, List, Any, Optional
from pathlib import Path
from os.path import dirname, realpath
import time


class RunnerConfig:
    ROOT_DIR = Path(dirname(realpath(__file__)))

    # ================================ USER SPECIFIC CONFIG ================================
    name: str = "new_runner_experiment"
    results_output_path: Path = ROOT_DIR / "experiments"
    operation_type: OperationType = OperationType.AUTO
    time_between_runs_in_ms: int = 1000

    def __init__(self):
        EventSubscriptionController.subscribe_to_multiple_events(
            [
                (RunnerEvents.BEFORE_EXPERIMENT, self.before_experiment),
                (RunnerEvents.BEFORE_RUN, self.before_run),
                (RunnerEvents.START_RUN, self.start_run),
                (RunnerEvents.START_MEASUREMENT, self.start_measurement),
                (RunnerEvents.INTERACT, self.interact),
                (RunnerEvents.STOP_MEASUREMENT, self.stop_measurement),
                (RunnerEvents.STOP_RUN, self.stop_run),
                (RunnerEvents.POPULATE_RUN_DATA, self.populate_run_data),
                (RunnerEvents.AFTER_EXPERIMENT, self.after_experiment),
            ]
        )
        self.run_table_model = None
        self.rapl_error_printed = False

    def create_run_table_model(self) -> RunTableModel:
        factor1 = FactorModel("quantization_type", ["2-bit", "3-bit", "4-bit"])
        self.run_table_model = RunTableModel(
            factors=[factor1],
            data_columns=[
                "avg_cpu_utilization",
                "avg_mem_utilization",
                "avg_cpu_power",
                "avg_mem_power",
                "cpu_energy_usage",
                "mem_energy_usage",
                "avg_gpu_power",
                "avg_gpu_utilization",
                "gpu_energy_usage",
            ],
        )
        return self.run_table_model

    def before_experiment(self) -> None:
        output.console_log("Config.before_experiment() called!")

    def before_run(self) -> None:
        output.console_log("Config.before_run() called!")

    def start_run(self, context: RunnerContext) -> None:
        output.console_log("Config.start_run() called!")

        # Start resource usage monitoring
        (
            avg_cpu_utilization,
            avg_mem_utilization,
            avg_cpu_power,
            avg_mem_power,
            cpu_energy_usage,
            mem_energy_usage,
            avg_gpu_power,
            avg_gpu_utilization,
            gpu_energy_usage,
        ) = self.monitor_usage()

        context.run_data = {
            "avg_cpu_utilization": avg_cpu_utilization,
            "avg_mem_utilization": avg_mem_utilization,
            "avg_cpu_power": avg_cpu_power,
            "avg_mem_power": avg_mem_power,
            "cpu_energy_usage": cpu_energy_usage,
            "mem_energy_usage": mem_energy_usage,
            "avg_gpu_power": avg_gpu_power,
            "avg_gpu_utilization": avg_gpu_utilization,
            "gpu_energy_usage": gpu_energy_usage,
        }
        output.console_log(f"Run data: {context.run_data}")

    def get_cpu_power(self):
        try:
            # 读取当前的能量消耗值，单位为微焦耳
            with open("/sys/class/powercap/intel-rapl:0/energy_uj", "r") as f:
                energy_start = int(f.read().strip())
            time.sleep(1)  # 等待1秒来计算能量消耗
            with open("/sys/class/powercap/intel-rapl:0/energy_uj", "r") as f:
                energy_end = int(f.read().strip())

            # 计算功率：功率 = 能量差 / 时间差
            energy_diff = energy_end - energy_start  # 微焦耳
            power_watts = energy_diff / 1e6  # 转换为瓦特
            return power_watts
        except FileNotFoundError:
            if not self.rapl_error_printed:
                print("RAPL 功率接口不可用")
                self.rapl_error_printed = True
            return 0.0

    def get_memory_power(self):
        try:
            # 读取内存（DRAM）的能量消耗值，单位为微焦耳
            with open("/sys/class/powercap/intel-rapl:1/energy_uj", "r") as f:
                energy_start = int(f.read().strip())
            time.sleep(1)  # 等待1秒来计算能量消耗
            with open("/sys/class/powercap/intel-rapl:1/energy_uj", "r") as f:
                energy_end = int(f.read().strip())

            # 计算功率：功率 = 能量差 / 时间差
            energy_diff = energy_end - energy_start  # 微焦耳
            power_watts = energy_diff / 1e6  # 转换为瓦特
            return power_watts
        except FileNotFoundError:
            if not self.rapl_error_printed:
                print("RAPL 内存功率接口不可用")
                self.rapl_error_printed = True
            return 0.0

    def monitor_cpu_memory(self):
        cpu_usage = []
        memory_usage = []
        cpu_power_usage = []
        mem_power_usage = []
        cpu_energy_usage = []
        mem_energy_usage = []

        # 读取初始能量消耗值
        try:
            with open("/sys/class/powercap/intel-rapl:0/energy_uj", "r") as f:
                initial_cpu_energy = int(f.read().strip())
            with open("/sys/class/powercap/intel-rapl:1/energy_uj", "r") as f:
                initial_mem_energy = int(f.read().strip())
        except FileNotFoundError:
            if not self.rapl_error_printed:
                print("RAPL 能量接口不可用")
                self.rapl_error_printed = True
            initial_cpu_energy = 0.0
            initial_mem_energy = 0.0

        start_time = time.time()
        while self.process.poll() is None and (time.time() - start_time) < 10:
            cpu_usage.append(psutil.cpu_percent(interval=1))
            memory_usage.append(psutil.virtual_memory().percent)

            cpu_power = self.get_cpu_power()
            if cpu_power is not None:
                cpu_power_usage.append(cpu_power)

            mem_power = self.get_memory_power()
            if mem_power is not None:
                mem_power_usage.append(mem_power)

        # 读取结束能量消耗值
        try:
            with open("/sys/class/powercap/intel-rapl:0/energy_uj", "r") as f:
                final_cpu_energy = int(f.read().strip())
            with open("/sys/class/powercap/intel-rapl:1/energy_uj", "r") as f:
                final_mem_energy = int(f.read().strip())
        except FileNotFoundError:
            if not self.rapl_error_printed:
                print("RAPL 能量接口不可用")
                self.rapl_error_printed = True
            final_cpu_energy = initial_cpu_energy
            final_mem_energy = initial_mem_energy

        # 计算能量消耗
        cpu_energy_usage = round(
            (final_cpu_energy - initial_cpu_energy) / 1e6, 3
        )  # 转换为焦耳
        mem_energy_usage = round(
            (final_mem_energy - initial_mem_energy) / 1e6, 3
        )  # 转换为焦耳

        avg_cpu = round(sum(cpu_usage) / len(cpu_usage), 3) if cpu_usage else 0
        avg_mem = round(sum(memory_usage) / len(memory_usage), 3) if memory_usage else 0
        avg_cpu_power = (
            round(sum(cpu_power_usage) / len(cpu_power_usage), 3)
            if cpu_power_usage
            else 0.0
        )
        avg_mem_power = (
            round(sum(mem_power_usage) / len(mem_power_usage), 3)
            if mem_power_usage
            else 0.0
        )

        return (
            avg_cpu,
            avg_mem,
            avg_cpu_power,
            avg_mem_power,
            cpu_energy_usage,
            mem_energy_usage,
        )

    def monitor_gpu(self):
        gpu_utilization = []
        gpu_power_usage = []

        start_time = time.time()
        while self.process.poll() is None and (time.time() - start_time) < 10:
            try:
                gpu_stats = subprocess.check_output(
                    [
                        "nvidia-smi",
                        "--query-gpu=power.draw,utilization.gpu,",
                        "--format=csv,nounits,noheader",
                    ],
                    encoding="utf-8",
                )

                for line in gpu_stats.strip().split("\n"):
                    power_draw, utilization = line.split(",")
                    gpu_power_usage.append(
                        float(power_draw.strip())
                    )  # 功耗，单位为瓦特
                    gpu_utilization.append(
                        float(utilization.strip())
                    )  # GPU使用率，单位为百分比

            except FileNotFoundError:
                print("nvidia-smi command not found. Skipping GPU monitoring.")
                return 0.0, 0.0, 0.0
            except subprocess.CalledProcessError as e:
                print(f"Error occurred while fetching GPU data: {e}")
                return 0.0, 0.0, 0.0

        # 计算能量消耗
        gpu_energy_usage = round(sum(gpu_power_usage) / 1e3, 3)  # 转换为焦耳
        avg_gpu_power = round(
            sum(gpu_power_usage) / len(gpu_power_usage) if gpu_power_usage else 0, 3
        )
        avg_gpu_utilization = round(
            sum(gpu_utilization) / len(gpu_utilization) if gpu_utilization else 0, 3
        )

        return avg_gpu_power, avg_gpu_utilization, gpu_energy_usage

    def monitor_usage(self):
        self.process = subprocess.Popen(
            ["python", "../2run_models/All-in-One.py"],
            #["python", "./primer.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=self.ROOT_DIR,
        )

        (
            avg_cpu,
            avg_mem,
            avg_cpu_power,
            avg_mem_power,
            cpu_energy_usage,
            mem_energy_usage,
        ) = self.monitor_cpu_memory()
        avg_gpu_power, avg_gpu_utilization, gpu_energy_usage = self.monitor_gpu()

        return (
            avg_cpu,
            avg_mem,
            avg_cpu_power,
            avg_mem_power,
            cpu_energy_usage,
            mem_energy_usage,
            avg_gpu_power,
            avg_gpu_utilization,
            gpu_energy_usage,
        )

    def start_measurement(self, context: RunnerContext) -> None:
        output.console_log("Config.start_measurement() called!")

    def interact(self, context: RunnerContext) -> None:
        output.console_log("Config.interact() called!")

    def stop_measurement(self, context: RunnerContext) -> None:
        output.console_log("Config.stop_measurement called!")

    def stop_run(self, context: RunnerContext) -> None:
        output.console_log("Config.stop_run() called!")

    def populate_run_data(
        self, context: RunnerContext
    ) -> Optional[Dict[str, SupportsStr]]:
        output.console_log("Config.populate_run_data() called!")
        return context.run_data

    def after_experiment(self) -> None:
        output.console_log("Config.after_experiment() called!")

    # ================================ DO NOT ALTER BELOW THIS LINE ================================
    experiment_path: Path = None
