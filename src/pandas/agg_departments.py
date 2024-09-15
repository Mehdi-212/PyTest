
import pandas as pd

def fonction_1_pandas():
    df = pd.DataFrame({
        'patient_id': [1, 2, 3, 4, 5],
        'age': [34, 45, 23, 64, 52],
        'department': ['Cardiology', 'Neurology', 'Cardiology', 'Orthopedics', 'Cardiology'],
        'visit_count': [10, 12, 5, 8, 9]
    })

    agg_df = df.groupby('department').agg({
        'visit_count': 'sum',
        'age': ['mean', 'max']
    }).reset_index()

    return agg_df
