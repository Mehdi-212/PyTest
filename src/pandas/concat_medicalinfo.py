import pandas as pd

def fonction_2_pandas():
    df = pd.DataFrame({
        'patient_name': ['John Doe', 'Jane Smith', 'Alice Brown'],
        'diagnosis': ['Diabetes', 'Heart Disease', 'Hypertension']
    })

    df['diagnosis_lower'] = df['diagnosis'].str.lower()
    df['full_info'] = df['patient_name'] + ' - ' + df['diagnosis_lower']

    return df
