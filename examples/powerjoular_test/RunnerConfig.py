from EventManager.Models.RunnerEvents import RunnerEvents
from EventManager.EventSubscriptionController import EventSubscriptionController
from ConfigValidator.Config.Models.RunTableModel import RunTableModel
from ConfigValidator.Config.Models.FactorModel import FactorModel
from ConfigValidator.Config.Models.RunnerContext import RunnerContext
from ConfigValidator.Config.Models.OperationType import OperationType
from ProgressManager.Output.OutputProcedure import OutputProcedure as output

from typing import Dict, List, Any, Optional
from pathlib import Path
from os.path import dirname, realpath

import os
import signal
import pandas as pd
import time
import subprocess
import shlex
import psutil


class RunnerConfig:
    ROOT_DIR = Path(dirname(realpath(__file__)))

    # ================================ USER SPECIFIC CONFIG ================================
    """The name of the experiment."""
    name: str = "new_runner_experiment"

    """The path in which Experiment Runner will create a folder with the name `self.name`, in order to store the
    results from this experiment. (Path does not need to exist - it will be created if necessary.)
    Output path defaults to the config file's path, inside the folder 'experiments'"""
    results_output_path: Path = ROOT_DIR / "experiments"

    """Experiment operation type. Unless you manually want to initiate each run, use `OperationType.AUTO`."""
    operation_type: OperationType = OperationType.AUTO

    """The time Experiment Runner will wait after a run completes.
    This can be essential to accommodate for cooldown periods on some systems."""
    time_between_runs_in_ms: int = 1000

    # Dynamic configurations can be one-time satisfied here before the program takes the config as-is
    # e.g. Setting some variable based on some criteria
    def __init__(self):
        """Executes immediately after program start, on config load"""

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
        self.run_table_model = None  # Initialized later
        self.run_data_list = []  # List to store run data rows
        output.console_log("Custom config loaded")

    def create_run_table_model(self) -> RunTableModel:
        """Create and return the run_table model here. A run_table is a List (rows) of tuples (columns),
        representing each run performed."""

        quantization_models = FactorModel(
            "quantization", ["8-bit", "16-bit", "32-bit"]
        )  # 这个地方会有多个factor，总数量为len(factors)的乘积

        # Define the columns in run_table for CPU and GPU metrics
        self.run_table_model = RunTableModel(
            factors=[quantization_models],
            data_columns=[
                "avg_cpu_utilization",  # CPU 平均使用率
                "avg_cpu_power",  # CPU 平均功耗
                "total_cpu_energy",  # CPU 总能耗
                "avg_gpu_utilization",  # GPU 平均使用率
                "avg_gpu_power",  # GPU 平均功耗
                "total_gpu_energy",  # GPU 总能耗
            ],
        )
        return self.run_table_model

    def before_experiment(self) -> None:
        """Perform any activity required before starting the experiment here
        Invoked only once during the lifetime of the program."""
        # 实验开始的准备步骤，不测能耗
        pass

    def before_run(self) -> None:
        """Perform any activity required before starting a run.
        No context is available here as the run is not yet active (BEFORE RUN)"""
        # 每次run开始前的准备步骤，不测能耗
        pass

    def start_run(self, context: RunnerContext) -> None:
        """Perform any activity required for starting the run here.
        For example, starting the target system to measure.
        Activities after starting the run should also be performed here."""

        # start the target (now changed to 'pressure.py')
        self.target = subprocess.Popen(
            ["python", "./pressure.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=self.ROOT_DIR,
        )

        # Log the start of the target process
        output.console_log(f"Started pressure.py with PID: {self.target.pid}")

    def start_measurement(self, context: RunnerContext) -> None:
        """Perform any activity required for starting measurements."""

        profiler_cmd = f'powerjoular -l -p {self.target.pid} -f {context.run_dir / "powerjoular.csv"}'

        time.sleep(1)  # allow the process to run a little before measuring
        self.profiler = subprocess.Popen(shlex.split(profiler_cmd))

    def interact(self, context: RunnerContext) -> None:
        """Perform any interaction with the running target system here, or block here until the target finishes."""

        # Monitor CPU usage
        process = psutil.Process(self.target.pid)
        start_time = time.time()
        while self.target.poll() is None:
            cpu_usage = process.cpu_percent(interval=1)
            output.console_log(f"Current CPU usage: {cpu_usage}%")

            # Check if 20 seconds have passed
            if time.time() - start_time > 20:
                self.target.terminate()  # Terminate the process after 20 seconds
                output.console_log("Terminated pressure.py after 20 seconds")
                break

        output.console_log("What to do next?")

    def stop_measurement(self, context: RunnerContext) -> None:
        """Perform any activity here required for stopping measurements."""

        os.kill(self.profiler.pid, signal.SIGINT)  # graceful shutdown of powerjoular
        self.profiler.wait()

    def stop_run(self, context: RunnerContext) -> None:
        """Perform any activity here required for stopping the run.
        Activities after stopping the run should also be performed here."""

        self.target.kill()
        self.target.wait()

    def log_run_data_to_csv(self, file_path: Path, run_data: Dict[str, Any]) -> None:
        """Append the run data to a CSV file."""

        # Check if the CSV file already exists
        if not file_path.exists():
            # If not, create it and write the header
            with open(file_path, mode="w") as file:
                file.write(",".join(run_data.keys()) + "\n")

        # Append the run data as a new row
        with open(file_path, mode="a") as file:
            file.write(",".join(map(str, run_data.values())) + "\n")

    def populate_run_data(self, context: RunnerContext) -> Optional[Dict[str, Any]]:
        """Parse and process any measurement data here and store in run_data_list."""

        # Read the powerjoular CSV file
        df = pd.read_csv(context.run_dir / f"powerjoular.csv-{self.target.pid}.csv")

        # Calculate the metrics (CPU/GPU usage and energy consumption)
        avg_cpu_utilization = (
            round(df["CPU Utilization"].mean(), 3)
            if "CPU Utilization" in df.columns
            else 0
        )
        avg_cpu_power = (
            round(df["CPU Power"].mean(), 3) if "CPU Power" in df.columns else 0
        )
        total_cpu_energy = (
            round(df["CPU Power"].sum(), 3) if "CPU Power" in df.columns else 0
        )
        avg_gpu_utilization = (
            round(df["GPU Utilization"].mean(), 3)
            if "GPU Utilization" in df.columns
            else 0
        )
        avg_gpu_power = (
            round(df["GPU Power"].mean(), 3) if "GPU Power" in df.columns else 0
        )
        total_gpu_energy = (
            round(df["GPU Power"].sum(), 3) if "GPU Power" in df.columns else 0
        )

        # Create a dictionary for the run data
        run_data = {
            "avg_cpu_utilization": avg_cpu_utilization,
            "avg_cpu_power": avg_cpu_power,
            "total_cpu_energy": total_cpu_energy,
            "avg_gpu_utilization": avg_gpu_utilization,
            "avg_gpu_power": avg_gpu_power,
            "total_gpu_energy": total_gpu_energy,
        }

        # Append the run data to the run_data_list
        self.run_data_list.append(run_data)

        # Optionally log the data to a CSV file
        self.log_run_data_to_csv(context.run_dir / "run_table.csv", run_data)

        return run_data

    def after_experiment(self) -> None:
        """After the experiment, transfer the run data to the run_table_model."""
        output.console_log("Experiment complete. Transferring data to run_table_model.")

        for run_data in self.run_data_list:
            # Extract data for the run_table_model and add a row
            row_data = (
                run_data["avg_cpu_utilization"],
                run_data["avg_cpu_power"],
                run_data["total_cpu_energy"],
                run_data["avg_gpu_utilization"],
                run_data["avg_gpu_power"],
                run_data["total_gpu_energy"],
            )
            try:
                # Assuming run_table_model has an add_row method
                self.run_table_model.add_row(row_data)
            except AttributeError:
                output.console_log(
                    "RunTableModel does not support adding rows directly."
                )

        output.console_log("Data transferred to RunTableModel.")

    # ================================ DO NOT ALTER BELOW THIS LINE ================================
    experiment_path: Path = None
