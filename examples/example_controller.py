import os
import subprocess
import importlib.util
import sys


def run_as_subprocess(file_path):
    """Run a Python file as separate subprocess"""
    try:
        result = subprocess.run(['python3', file_path], check=True, capture_output=True, text=True)
        print(f"✅ {file_path} executed successfully\nOutput:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error in {file_path}\nError:\n{e.stderr}")


def run_as_module(file_path):
    """Import and run the file as a module"""
    module_name = os.path.splitext(os.path.basename(file_path))[0]
    try:
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        print(f"✅ {file_path} executed as module")
    except Exception as e:
        print(f"❌ Error importing {file_path}: {str(e)}")


def main():
    scripts = [
        "1.1. create_super_table.py",
        "1.2. create_roles.py",
        "1.3. create_users.py",
        "2.1. write_dummy_data.py",
        "2.2. write_single_data.py",
        "2.3. write_staging.py",
        "2.4.2. write_monitoring_parallel.py",
        "2.4.1. write_monitoring_simple.py",
        "3.1. read_data_error.py",
        "3.2.1. read_super_data_ok.py",
        "3.2.2. read_table_data_ok.py",
        "3.3. read_meta.py",
        "3.4. read_staging.py",
        "3.5. read_query_plans.py",
        "3.6. read_user.py",
        "4.1. clean_obsolete_files.py",
        #"5.1. delete_table.py",
        #"5.2. delete_super_table.py",
    ]

    print("🚀 Starting script execution...\n")

    for script in scripts:
        print(f"🏃 Running {script}...")
        # Choose one method:
        run_as_subprocess(script)  # Recommended for most cases
        # OR
        # run_as_module(script)    # Alternative if you need shared state

    print("\n🎉 All scripts executed!")


if __name__ == "__main__":
    main()