# uu-bpk-scrapper
```/scrap```: Get all PDFs from BPK website, convert it to text, and save the results to Azure Cloud Storage  
```/chunk```: Decompose each laws into articles and save the results to Azure Cloud Storage 
```/status```: Get all laws relationships (status) and save it as two relational tables (uu & status) in CSV format

---

# Project Setup Guide

This guide provides the steps to set up, configure, and run the script across multiple machines.

## Prerequisites

Before proceeding, ensure that the following software is installed on your system:

- **Python 3** (Ensure that `python3` is installed by running `python3 --version`)
- **pip** (Python package installer)
- **venv** (Python virtual environment module)
- **GNU nano** (Optional: text editor, replaceable with any other editor)

## Setup Instructions

Follow these steps to set up and run the project:

### 1. Update Package List

First, update your system's package list to ensure that you are installing the latest versions of packages:

```bash
sudo apt update
```

### 2. Install `pip` and `venv`

Next, install `python3-pip` and `python3-venv`:

```bash
sudo apt install python3-pip
sudo apt install python3-venv
```

### 3. Create and Activate a Virtual Environment

To keep dependencies isolated, it's a good practice to use a virtual environment. Create a new virtual environment named `test`:

```bash
python3 -m venv test
```

Activate the virtual environment:

```bash
source test/bin/activate
```

### 4. Install Project Dependencies

Create a `requirements.txt` file with all the required Python packages or copy an existing one. After creating it, install all the dependencies listed in `requirements.txt` using `pip`:

```bash
nano requirements.txt
pip install -r requirements.txt
```

Ensure that `requirements.txt` contains all the necessary libraries.

### 5. Create the Python Script

Create the main Python script named `code.py` by editing it in `nano` or another text editor:

```bash
nano code.py
```

Write your Python code inside this file.

### 6. Create the Shell Script

Create a shell script named `shell.sh` that automates running the Python script or any other setup/command:

```bash
nano shell.sh
```

Ensure that the `shell.sh` file includes the necessary commands to run the project.

### 7. Make the Shell Script Executable

Give the shell script execute permissions:

```bash
chmod +x shell.sh
```

### 8. Run the Shell Script in the Background

To run the script in the background (detached from the terminal), use the `nohup` command:

```bash
nohup ./shell.sh &
```

This will start the shell script and continue running even if you log out of the session.

### 9. View Output Logs

To monitor the output of the background process, you can view the last 2000 lines of the `nohup.out` file, which contains the output from `nohup`:

```bash
tail -n 2000 nohup.out
```

### 10. Deactivating the Virtual Environment

Once you are done working, deactivate the virtual environment by running:

```bash
deactivate
```

---

## Notes

- Ensure that `requirements.txt` contains all the required Python packages.
- The `nohup.out` file stores the output of the background process. If the output is large, consider using `grep` or similar commands to filter the logs.
