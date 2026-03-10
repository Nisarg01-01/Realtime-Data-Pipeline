#!/usr/bin/env python
"""
Quick Health Check Script for Real-Time E-Commerce Data Pipeline

Run this script to diagnose setup issues instantly.
"""

import os
import sys
import subprocess
from pathlib import Path


def check_python_version():
    """Check Python version"""
    version = sys.version_info
    print(f"Python Version: {version.major}.{version.minor}.{version.micro}", end="")
    if version.major == 3 and version.minor >= 11:
        print(" [OK]")
        return True
    else:
        print(" [FAIL] (need 3.11+)")
        return False


def check_conda_env():
    """Check if conda environment is active"""
    env = os.environ.get('CONDA_DEFAULT_ENV')
    if env:
        print(f"Conda Environment: {env} [OK]")
        return True
    else:
        print("Conda Environment: Not active [FAIL]")
        print("  Run: conda activate realtime-pipeline")
        return False


def check_pyspark_vars():
    """Check if PySpark environment variables are set"""
    pyspark_python = os.environ.get('PYSPARK_PYTHON')
    pyspark_driver = os.environ.get('PYSPARK_DRIVER_PYTHON')
    
    print("\nPySpark Environment Variables:")
    
    if pyspark_python:
        print(f"  PYSPARK_PYTHON: Set ✓")
    else:
        print(f"  PYSPARK_PYTHON: NOT set ✗")
        print(f"    Run: .\\setup-system.ps1 to configure PySpark")
        return False
    
    if pyspark_driver:
        print(f"  PYSPARK_DRIVER_PYTHON: Set ✓")
    else:
        print(f"  PYSPARK_DRIVER_PYTHON: NOT set ✗")
        return False
    
    return True


def check_packages():
    """Check critical packages"""
    packages = [
        ('pyspark', 'pyspark'),
        ('kafka_python', 'kafka'),
        ('pandas', 'pandas'),
        ('sqlalchemy', 'sqlalchemy'),
        ('psycopg2', 'psycopg2'),
        ('streamlit', 'streamlit'),
    ]
    
    print("\nCritical Packages:")
    all_ok = True
    for display_name, import_name in packages:
        try:
            __import__(import_name)
            print(f"  {display_name}: ✓")
        except ImportError:
            print(f"  {display_name}: ✗ (Run: pip install -r requirements.txt)")
            all_ok = False
    
    return all_ok


def check_data_file():
    """Check if data file exists"""
    print("\nData Files:")
    data_file = Path("Data/2019-Oct.csv")
    if data_file.exists():
        size_mb = data_file.stat().st_size / (1024 * 1024)
        print(f"  Data/2019-Oct.csv: Exists ({size_mb:.1f} MB) ✓")
        return True
    else:
        print(f"  Data/2019-Oct.csv: NOT found ✗")
        print("    Download from: https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store")
        return False


def check_java():
    """Check Java installation"""
    print("\nJava (required for PySpark):")
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            version_line = (result.stderr or result.stdout).split('\n')[0]
            print(f"  Java: {version_line[:50]}... ✓")
            return True
    except:
        pass
    
    print("  Java: NOT found ✗")
    print("    Install from: https://adoptium.net/")
    return False


def main():
    print("=" * 60)
    print("Real-Time E-Commerce Pipeline - Health Check")
    print("=" * 60)
    print()
    
    checks = [
        ("Python Version", check_python_version),
        ("Conda Environment", check_conda_env),
        ("PySpark Variables", check_pyspark_vars),
        ("Critical Packages", check_packages),
        ("Data File", check_data_file),
        ("Java Installation", check_java),
    ]
    
    results = {}
    for name, check_fn in checks:
        results[name] = check_fn()
        print()
    
    print("=" * 60)
    print("Summary:")
    passed = sum(results.values())
    total = len(results)
    
    for name, result in results.items():
        status = "✓" if result else "✗"
        print(f"  {status} {name}")
    
    print()
    print(f"Status: {passed}/{total} checks passed")
    
    if passed == total:
        print("\n✓ All systems operational!")
        print("\nYou can now run:")
        print("  pytest tests/ -v          # Run all tests")
        print("  python producer.py        # Run producer")
        return 0
    else:
        print("\n✗ Some checks failed. See above for fixes.")
        print("\nFor detailed troubleshooting, see: SETUP_GUIDE.md")
        return 1


if __name__ == "__main__":
    sys.exit(main())
