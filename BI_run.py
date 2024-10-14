import importlib.util
spec = importlib.util.spec_from_file_location("Anchor Strategy.anchor_bi",
                                              'G:/PerfInfo/Performance Management/OR Team/BI Reports/Anchor Strategy/anchor_bi.py')
module = importlib.util.module_from_spec(spec)
import pandas as pd
spec.loader.exec_module(module)

employees, pension, banding, staff_groups, age_bands = module.anchor_strategy()