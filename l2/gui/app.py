"""DeepTrade L2 desktop application entry point.

Usage:
    python -m l2.gui.app
    python l2/gui/app.py
"""

import sys
from pathlib import Path

# Ensure project root is on path
_project_root = Path(__file__).parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QFont

from l2.data.config import L2Config
from l2.gui.main_window import MainWindow


def main():
    """Launch the DeepTrade L2 desktop application."""
    app = QApplication(sys.argv)
    app.setApplicationName("DeepTrade L2")
    app.setOrganizationName("DeepTrade")

    # Dark theme palette
    app.setFont(QFont("Microsoft YaHei", 10))
    app.setStyle("Fusion")
    app.setStyleSheet("""
        /* === Design System: DeepTrade Dark ===
           Surface levels: base #12131c, raised #1a1b26, elevated #242540
           Borders: #2a2b3d, accent #3b82f6
           Text: primary #e2e4f0, secondary #8e91a8, muted #5b5e76
        */

        /* Global */
        QMainWindow { background: #12131c; }
        QWidget { font-family: "Microsoft YaHei", "Segoe UI", sans-serif; }

        /* Menus & Tooltips */
        QToolTip { color: #e2e4f0; background: #242540; border: 1px solid #2a2b3d;
                   padding: 4px 8px; border-radius: 4px; }
        QMenu { background: #242540; color: #e2e4f0; border: 1px solid #2a2b3d;
                padding: 4px; }
        QMenu::item { padding: 6px 24px; }
        QMenu::item:selected { background: #3b82f6; }
        QMenu::separator { height: 1px; background: #2a2b3d; margin: 4px 8px; }

        /* Status Bar */
        QStatusBar { background: #1a1b26; color: #8e91a8; border-top: 1px solid #2a2b3d; }

        /* Scrollbars */
        QScrollBar:vertical { background: #12131c; width: 8px; border: none; }
        QScrollBar::handle:vertical { background: #3b3d54; border-radius: 4px; min-height: 24px; }
        QScrollBar::handle:vertical:hover { background: #4b4d6a; }
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
        QScrollBar:horizontal { background: #12131c; height: 8px; border: none; }
        QScrollBar::handle:horizontal { background: #3b3d54; border-radius: 4px; min-width: 24px; }
        QScrollBar::handle:horizontal:hover { background: #4b4d6a; }
        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { width: 0; }

        /* Dock Widgets */
        QDockWidget { color: #e2e4f0; background: #12131c; }
        QDockWidget::title { background: #1a1b26; padding: 6px 12px;
                             border-left: 3px solid #3b82f6;
                             border-bottom: 1px solid #2a2b3d; }
        QDockWidget::close-button, QDockWidget::float-button {
            background: transparent; border: none; padding: 2px; }

        /* ToolBar */
        QToolBar { background: #1a1b26; border-bottom: 1px solid #2a2b3d;
                   spacing: 4px; padding: 4px 8px; }
        QToolBar::separator { width: 1px; background: #2a2b3d; margin: 4px 8px; }
        QToolButton { color: #e2e4f0; padding: 4px 10px; border-radius: 4px; }
        QToolButton:hover { background: #242540; }
        QToolButton:pressed, QToolButton:checked { background: #3b82f6; }

        /* Dialogs */
        QInputDialog QLineEdit { background: #1a1b26; color: #e2e4f0;
                                 border: 1px solid #2a2b3d; padding: 6px 8px;
                                 border-radius: 4px; }
        QInputDialog QLabel { color: #e2e4f0; }
        QInputDialog QPushButton { background: #3b82f6; color: white; border: none;
                                   padding: 6px 16px; border-radius: 4px; }
    """)

    # Parse CLI args for optional config
    config = L2Config()
    if "--tdx-dir" in sys.argv:
        idx = sys.argv.index("--tdx-dir")
        if idx + 1 < len(sys.argv):
            config.TDX_DIR = sys.argv[idx + 1]
    if "--custom-ex-hosts" in sys.argv:
        idx = sys.argv.index("--custom-ex-hosts")
        if idx + 1 < len(sys.argv):
            # Format: "name,ip,port;name,ip,port"
            hosts_str = sys.argv[idx + 1]
            hosts = []
            for h in hosts_str.split(";"):
                parts = h.split(",")
                if len(parts) == 3:
                    hosts.append((parts[0].strip(), parts[1].strip(), int(parts[2].strip())))
            if hosts:
                config.CUSTOM_EX_HOSTS = hosts
                print(f"Using custom EX_HOSTS: {config.CUSTOM_EX_HOSTS}")

    window = MainWindow(config)
    window.show()

    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
