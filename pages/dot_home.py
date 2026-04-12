import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.ui.dot_dashboard import render_dot_dashboard

render_dot_dashboard()
