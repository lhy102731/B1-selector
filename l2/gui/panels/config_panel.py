"""Configuration panel — enable/disable analyzers and adjust thresholds."""

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QGroupBox, QCheckBox,
    QSlider, QLabel, QHBoxLayout, QPushButton
)
from PyQt5.QtCore import Qt, pyqtSignal


class ConfigPanel(QWidget):
    """Analysis configuration panel.

    Controls:
      - Analyzer toggles (enable/disable each signal type)
      - Threshold sliders (big order, order wall, anomaly)
      - Start/Stop button
    """

    config_changed = pyqtSignal(dict)   # emits updated config dict

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout()

        # Analyzer toggles
        self.toggle_group = QGroupBox("Signal Detection")
        self.toggle_group.setStyleSheet(
            "QGroupBox { color: #e2e4f0; font-weight: bold; border: 1px solid #2a2b3d;"
            " padding: 12px 8px 8px 8px; margin-top: 12px; border-radius: 4px; }"
            "QGroupBox::title { subcontrol-origin: margin; left: 8px; padding: 0 4px; }"
        )
        toggle_layout = QVBoxLayout()

        self.cb_whale_acc = QCheckBox("Whale Accumulation (鲸鱼吸筹)")
        self.cb_whale_dist = QCheckBox("Whale Distribution (鲸鱼出货)")
        self.cb_wash = QCheckBox("Wash Trading (对倒)")
        self.cb_order_wall = QCheckBox("Order Wall (委托墙)")
        self.cb_anomaly = QCheckBox("Anomaly Trade (异常成交)")
        self.cb_surge = QCheckBox("Tick Surge (成交放量)")
        self.cb_depth = QCheckBox("Depth Imbalance (深度失衡)")

        for cb in [self.cb_whale_acc, self.cb_whale_dist, self.cb_wash,
                    self.cb_order_wall, self.cb_anomaly, self.cb_surge, self.cb_depth]:
            cb.setChecked(True)
            cb.setStyleSheet(
                "QCheckBox { color: #e2e4f0; spacing: 6px; font-size: 11px; }"
                "QCheckBox::indicator { width: 14px; height: 14px; }"
            )
            toggle_layout.addWidget(cb)

        self.toggle_group.setLayout(toggle_layout)
        layout.addWidget(self.toggle_group)

        # Threshold controls
        self.threshold_group = QGroupBox("Thresholds")
        self.threshold_group.setStyleSheet(
            "QGroupBox { color: #e2e4f0; font-weight: bold; border: 1px solid #2a2b3d;"
            " padding: 12px 8px 8px 8px; margin-top: 12px; border-radius: 4px; }"
            "QGroupBox::title { subcontrol-origin: margin; left: 8px; padding: 0 4px; }"
        )
        thresh_layout = QVBoxLayout()

        self.sliders = {}
        slider_configs = [
            ("big_order_pct", "Big Order %ile", 50, 99, 85),
            ("wall_ratio", "Order Wall Ratio", 2.0, 10.0, 3.0),
            ("anomaly_zscore", "Anomaly Z-Score", 2.0, 8.0, 4.0),
        ]

        for key, label, min_v, max_v, default in slider_configs:
            row = QHBoxLayout()
            is_float = isinstance(min_v, float)
            lbl = QLabel(f"{label}: {default}")
            lbl.setStyleSheet("color: #8e91a8; font-size: 11px;")
            slider = QSlider(Qt.Horizontal)
            if is_float:
                slider.setRange(int(min_v * 10), int(max_v * 10))
                slider.setValue(int(default * 10))
            else:
                slider.setRange(min_v, max_v)
                slider.setValue(default)

            slider.setStyleSheet("""
                QSlider::groove:horizontal { height: 4px; background: #2a2b3d;
                    border-radius: 2px; }
                QSlider::handle:horizontal { width: 14px; height: 14px;
                    background: #3b82f6; border-radius: 7px; margin: -5px 0; }
                QSlider::handle:horizontal:hover { background: #60a5fa; }
            """)

            def make_handler(lbl, lab, f, mv, mn):
                def handler(v):
                    if f:
                        lbl.setText(f"{lab}: {v / 10.0:.1f}")
                    else:
                        lbl.setText(f"{lab}: {v}")
                return handler

            slider.valueChanged.connect(make_handler(lbl, label, is_float, min_v, default))

            row.addWidget(lbl)
            row.addWidget(slider)
            thresh_layout.addLayout(row)
            self.sliders[key] = (slider, min_v, max_v)

        self.threshold_group.setLayout(thresh_layout)
        layout.addWidget(self.threshold_group)

        # Buttons
        btn_layout = QHBoxLayout()
        self.btn_apply = QPushButton("Apply")
        self.btn_apply.setStyleSheet(
            "QPushButton { background: #3b82f6; color: white; border: none;"
            " padding: 6px 16px; border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background: #2563eb; }"
        )
        self.btn_reset = QPushButton("Reset")
        self.btn_reset.setStyleSheet(
            "QPushButton { background: #2a2b3d; color: #e2e4f0; border: none;"
            " padding: 6px 16px; border-radius: 4px; }"
            "QPushButton:hover { background: #3b3d54; }"
        )
        btn_layout.addWidget(self.btn_apply)
        btn_layout.addWidget(self.btn_reset)
        layout.addLayout(btn_layout)

        layout.addStretch()
        self.setLayout(layout)

        # Connect signals
        self.btn_apply.clicked.connect(self._emit_config)
        self.btn_reset.clicked.connect(self._reset_defaults)

    def _emit_config(self):
        """Collect current config state and emit."""
        config = {
            "whale_accumulation": self.cb_whale_acc.isChecked(),
            "whale_distribution": self.cb_whale_dist.isChecked(),
            "wash_trading": self.cb_wash.isChecked(),
            "order_wall": self.cb_order_wall.isChecked(),
            "anomaly_trade": self.cb_anomaly.isChecked(),
            "tick_surge": self.cb_surge.isChecked(),
            "depth_imbalance": self.cb_depth.isChecked(),
        }
        for key, (slider, min_v, max_v) in self.sliders.items():
            if isinstance(min_v, float) and min_v < 1:
                config[key] = slider.value() / 10.0
            else:
                config[key] = slider.value()
        self.config_changed.emit(config)

    def _reset_defaults(self):
        """Reset all controls to defaults."""
        for cb in [self.cb_whale_acc, self.cb_whale_dist, self.cb_wash,
                    self.cb_order_wall, self.cb_anomaly, self.cb_surge, self.cb_depth]:
            cb.setChecked(True)
        defaults = {"big_order_pct": 85, "wall_ratio": 3.0, "anomaly_zscore": 4.0}
        for key, (slider, min_v, max_v) in self.sliders.items():
            default = defaults.get(key, max_v // 2)
            if isinstance(min_v, float) and min_v < 1:
                slider.setValue(int(default * 10))
            else:
                slider.setValue(int(default))
