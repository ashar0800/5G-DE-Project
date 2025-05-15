# orchestrator.py
from prefect import flow, task
import subprocess
import time
import os

# Define each component as a Prefect task
@task
def start_producer():
    print("Starting producer.py")
    subprocess.Popen(["python", "producer.py"])
    time.sleep(30)  # allow it to generate enough raw data

@task
def start_processor():
    print("Starting processor.py")
    subprocess.Popen(["python", "processor.py"])
    time.sleep(10)

@task
def start_trainer():
    print("Starting trainer.py")
    subprocess.Popen(["python", "trainer.py"])
    time.sleep(10)

@task
def start_deployer():
    print("Starting deployer.py")
    subprocess.Popen(["python", "deployer.py"])
    time.sleep(10)

@task
def start_drift_detector():
    print("Starting drift_detector.py")
    subprocess.Popen(["python", "drift_detector.py"])
    time.sleep(10)

@task
def start_alerter():
    print("Starting alerter.py")
    subprocess.Popen(["python", "alerter.py"])
    time.sleep(5)

@task
def start_allocator():
    print("Starting allocator.py")
    subprocess.Popen(["python", "allocator.py"])
    time.sleep(5)

@task
def start_ui():
    print("Starting ui.py")
    subprocess.Popen(["streamlit", "run", "ui.py"])
    time.sleep(5)

# Main Prefect flow
@flow(name="5G Resource Allocation Pipeline")
def main_flow():
    # Start in order, respecting dependencies and allowing headroom
    producer_future = start_producer()
    processor_future = start_processor(wait_for=[producer_future])
    trainer_future = start_trainer(wait_for=[processor_future])
    deployer_future = start_deployer(wait_for=[trainer_future, processor_future])
    drift_detector_future = start_drift_detector(wait_for=[deployer_future])
    alerter_future = start_alerter(wait_for=[drift_detector_future])
    allocator_future = start_allocator(wait_for=[deployer_future])
    ui_future = start_ui(wait_for=[processor_future, deployer_future, alerter_future, allocator_future])

if __name__ == "__main__":
    main_flow()
