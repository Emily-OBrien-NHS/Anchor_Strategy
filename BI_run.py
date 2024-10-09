import os
os.chdir('G:/PerfInfo/Performance Management/OR Team/BI Reports/Anchor Strategy')
from anchor_bi import anchor_strategy

employees, pension, banding, staff_groups, age_bands = anchor_strategy()