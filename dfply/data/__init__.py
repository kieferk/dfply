import os
import pandas as pd

root = os.path.abspath(os.path.dirname(__file__))
diamonds = pd.read_csv(os.path.join(root, "diamonds.csv"))
