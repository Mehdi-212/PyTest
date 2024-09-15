import pandas as pd
import numpy as np

def fonction_5_pandas():
    df = pd.DataFrame({
        'patient_id': [1, 2, 3, 4, 5],
        'age': [34, np.nan, 50, np.nan, 15],
        'department': ['Cardiology', 'Neurology', 'Orthopedics', np.nan, 'Neurology']
    })

    df['age'].fillna(df['age'].mean(), inplace=True)
    df['department'].fillna('Unknown', inplace=True)

    return df
