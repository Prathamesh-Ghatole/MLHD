# A master script to clean and export MLHD data!

""" Algoirithm:

1. Load libraries
2. Get config
3. Load Files
4. 
"""

# Essential Imports
from lib import io_ as io
from time import monotonic
from rich.progress import track

# For pretty CLI
from rich import print
from rich.console import Console
console = Console()
console.clear()

# Getting started
io.generate_folders()
master_start = monotonic()
OUTPUT_LOG = {}

# Loading ENV variables
console.log("Loading ENV variables...")
ENV = io.get_config()

MLHD_ROOT = ENV['MLHD_ROOT']
WRITE_ROOT = ENV['WRITE_ROOT']
LOG_WRITE_PATH = ENV['LOG_WRITE_PATH']
LOG_EPOCH = ENV['LOG_EPOCH']

# 1 Dimensional list of MLHD file paths
console.log("Generating MLHD Paths...")
MLHD_PATHS = io.generate_paths(MLHD_ROOT)

