import pandas as pd
import os
import opendatasets as od

ecommerce_datasets = 'https://www.kaggle.com/datasets/aliessamali/ecommerce'
od.download(ecommerce_datasets)

#verify downloaded dataset
df = pd.read_csv('Dataset.csv')
