import streamlit as st
import os
import sys

# Add the current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Import and run the memory-safe application
from memory_safe_analyzer import main

if __name__ == "__main__":
    main()