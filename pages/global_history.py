import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import PORTFOLIOS
from app.ui.history_page import render_history

render_history(PORTFOLIOS["global"])
