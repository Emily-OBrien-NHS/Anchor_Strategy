import importlib.util
spec = importlib.util.spec_from_file_location("Anchor Strategy.PY_PBI0021_FACT_AnchorStrategy",
                                              'G:/PerfInfo/Performance Management/OR Team/BI Reports/Anchor Strategy/PY_PBI0021_FACT_AnchorStrategy.py')
module = importlib.util.module_from_spec(spec)
import pandas as pd
spec.loader.exec_module(module)

employees, pension, banding, staff_groups, age_bands = module.anchor_strategy()