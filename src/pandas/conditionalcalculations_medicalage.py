import pandas as pd

def fonction_3_pandas():
    df = pd.DataFrame({
        'patient_id': [1, 2, 3, 4, 5],
        'age': [34, 70, 50, 20, 15],
        'department': ['Cardiology', 'Neurology', 'Orthopedics', 'Cardiology', 'Neurology']
    })

    df['age_category'] = df['age'].apply(lambda x: 'senior' if x > 60 else 'adult' if x > 18 else 'minor')

    return df
