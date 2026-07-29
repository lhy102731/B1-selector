# Brick Research Architecture Notes

Brick V2 research must keep production and research separate. Production
`backtest_brick_v2.py` is not modified by AG2 experiments. Research runners
read rebuilt candidate parquet/raw parquet inputs and write under
`research_state/brick/`.

Current preferred loop:

1. AG2-KBase roundtable proposes a new mechanism.
2. Validator checks archived factors, validation boundaries, and production
   separation.
3. Research-only runner implements the mechanism.
4. Strict rolling forward validation reports both Signal Quality NAV and
   executable NAV.
5. Results and failed ideas are written to factor memory to prevent repeats.
