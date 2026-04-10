# run_pipeline.py
import sys
from pathlib import Path

# Add src/ to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pipeline.rag_pipeline import run_pipeline

if __name__ == "__main__":
    run_pipeline()