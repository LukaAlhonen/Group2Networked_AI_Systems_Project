from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd

def obtain_categorical_pattern(df):
    # Calculate the pattern of features ['service','proto','conn_state','history'] in a dataframe
    
    categorical_features = ['service','proto','conn_state','history']
    pattern = []
    for column in categorical_features:
        proportions = df[column].value_counts(normalize=True)
        pattern.append(list(zip(proportions.index, proportions.values)))
    flattened_pattern = [(item[0], item[1]) for sublist in pattern for item in sublist]
    series_pattern = pd.Series(dict(flattened_pattern))
    return series_pattern

def str_to_series(s):
    lines = s.strip().split('\n')
    lines = lines[:len(lines)-1]
    # Extract index and values from each line
    index = []
    values = []
    for line in lines:
        parts = line.split()
        index.append(parts[0])  # Assuming the index is the first part
        values.append(float(parts[1]))  # Assuming the value is the second part
    
    # Create a pandas Series
    series = pd.Series(values, index=index)
    
    return series

def evaluate_categorical_similarity(s1, s2):
    # s1 is new pattern calculated from obtain_categorical_pattern and s1 is str read from database.
    s2 = str_to_series(s2)
    all_indices = s1.index.union(s2.index)
    s1 = s1.reindex(all_indices, fill_value=0)
    s2 = s2.reindex(all_indices, fill_value=0)
    sim = cosine_similarity(s1.values.reshape(1, -1), s2.values.reshape(1, -1))
    return sim