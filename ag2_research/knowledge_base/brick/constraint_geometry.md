# Brick Constraint Geometry

The active constraint boundary is chronological:

- split by `entry_date`;
- purge train labels with `exit_date < test_start`;
- use train/validation only for choices;
- evaluate test years once as unseen;
- no random splits or same-period optimization.

Feature boundary:

- allowed: signal-day and prior data;
- allowed: `entry_date` open only versus signal-day known references, matching
  `daily_select.py`;
- forbidden as model inputs: `return_pct`, `exit_date`, `exit_price`,
  `hold_days`, entry-day high/low/close, and T+1 close-derived indicators.
