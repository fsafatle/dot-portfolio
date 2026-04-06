import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import PORTFOLIOS
from app.ui.allocations_page import render_allocations

render_allocations(PORTFOLIOS["brazil"])
