"""
Description of .parquet file:
1. community_id:    hash-like strings derived from (src_id, dst_id, src_port, dst_port), simplifying the correlation of flow-level logs
2. conn_state:      describe connection
    --- S0      - attempt seen, no reply
    --- S1      - established, not terminated
    --- SF      - Normal establishment & termination
    --- REJ     - attemp rejected
    --- S2      - established, close attempt by originator (no reply from responder)
    --- S3      - established, close attempt by responder (no reply from originator)
    --- RSTO    - established, originator aborted
    --- RSTR    - established, responder aborted
    --- RSTOS0  - Originator sent a SYN followed by a RST, never saw a SYN-ACK from the responder
    --- RSTRH   - Responder sent a SYN ACK followed by a RST, never saw a SYN from the (purported) originator.
    --- SH      - Originator sent a SYN followed by a FIN, we never saw a SYN ACK from the responder (hence the connection was “half” open)
    --- SHR     - Responder sent a SYN ACK followed by a FIN, we never saw a SYN from the originator.
    --- OTH     - No SYN seen, just midstream traffic
3. duration:        how long the connection last
4. history: record state history of connections as a string of letters
    --- s - a SYN with the ACK bit set
    --- h - a SYN + ACK
    --- a - a pure ACK
    --- d - packet with payload
    --- f - packet with FIN bit set
    --- r - packet with RST bit set
    --- c - packet with a bad checksum
    --- g - a content gap
    --- t - packet with retransmitted payload
    --- w - packet with a zero window advertisement
    --- i - inconsistent packet (FIN + RST)
    --- q - multi-flag packet (SYN + FIN or SYN + RST)
    --- ^ - connection direction was flipped by Zeek's heuristic
    --- x - connection analysis partial
5,6,7,8 src_ip_zeek, src_port_zeek, dest_ip_zeek, dest_port_zeek
9. local_orig:      True if connection is originated locally
10. local_resp:     True if connection is responded to locally  
11. missed_bytes:   number of bytes missed in content gaps
12. orig_bytes:     number of payload bytes the originator sent 
13. orig_ip_bytes:  number of IP level bytes that the originator sent
14. orig_pkts:      number of packets that the originator sent
15. proto:          transport layer protocol
16. resp_bytes:     number of payload bytes the responder sent
17. resp_ip_bytes:  number of IP level bytes that the responder sent
18. resp_pkts:      number of packets that the responder sent
19. service:        identification of an application protocol being sent over the connection
20. ts:             time of the first packet
21. uid:            unique identifier of the connection.
22. datetime:       yyyy-mm-dd hh-mm-ss.xxxxxx
23. label_tactic
"""

import pandas as pd
import numpy as np
from itertools import chain
from sklearn.pipeline import make_pipeline, Pipeline
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import MinMaxScaler, FunctionTransformer, OneHotEncoder, RobustScaler
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.model_selection import GridSearchCV
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import RidgeClassifier
from similary_calculation import obtain_categorical_pattern, evaluate_categorical_similarity
import uuid
import pickle
from pipeline_metric_manage import retrieve_by_hthres, retrieve_by_lthres, retrieve_by_pid, retrieve_table_name
from schedule import schedule

class Pipeline_info:
    def __init__(self, id: str, pattern: pd.Series, accuracy: float, param: dict, pipeline: Pipeline) -> None:
        self.id = id
        self.pattern = pattern
        self.accuracy = accuracy
        self.pipeline = pipeline

# Duration --- Impute & 0-indicator, log10, MinMaxScaler
def get_lg10_for_duration(x):
    res = np.log10(x)
    res[np.isinf(res)] = -7
    return res

# missing_bytes --- Impute(0), MinMaxScaler
def minmax_scaling_for_missing_bytes(x):
    largest_value = max(x)
    if largest_value == 0:
        return x
    else:
        s = min(x)
        scaled_array = [(v - s) / largest_value - s for v in x]
        return scaled_array

#  local_orig, local_resp --- BoolToInt, Impute(0)
def bool_to_numeric(arr):
    return arr.astype(int)

def preprocessing_pipeline() -> Pipeline:
    # df is the dataframe that contains all features

    # Service, Proto, Conn_state, history --- Impute('none'), OneHot('ignore')
    category_pipeline = make_pipeline(SimpleImputer(strategy='constant',fill_value='none'),OneHotEncoder(handle_unknown="ignore"),)
    
    duration_pipeline = make_pipeline(
        SimpleImputer(strategy='constant',fill_value=0,add_indicator=True),
        FunctionTransformer(get_lg10_for_duration, feature_names_out="one-to-one"),
        MinMaxScaler(feature_range=(0, 1)),
    )

    # orig_ip_bytes, resp_ip_bytes, orig_pkts, resp_pkts, orig_bytes, resp_bytes --- Impute(0), MinMaxScaler
    numerical_pipeline = make_pipeline(
        SimpleImputer(strategy='constant',fill_value=0),
        MinMaxScaler(feature_range=(0, 1)),
    )  

    robust_numerical_pipeline = make_pipeline(
        SimpleImputer(strategy='constant',fill_value=0),
        RobustScaler(with_centering=True, with_scaling=True),
    )
        
    missing_bytes_pipeline = make_pipeline(
        SimpleImputer(strategy='constant',fill_value=0),
        MinMaxScaler(feature_range=(0, 1)),
        #FunctionTransformer(minmax_scaling_for_missing_bytes, feature_names_out="one-to-one")
    )
    
    local_pipeline = make_pipeline(
        FunctionTransformer(bool_to_numeric,validate=False, feature_names_out="one-to-one"),
        SimpleImputer(strategy='constant',fill_value=0),
    )
        
    preprocessing = ColumnTransformer([
        ('cat', category_pipeline, ['service','proto','conn_state','history']),
        ('duration', duration_pipeline, ['duration']),
        ('bytes', robust_numerical_pipeline, ['orig_ip_bytes','orig_pkts','resp_ip_bytes','resp_pkts','orig_bytes','resp_bytes']),
        ('missing_value', missing_bytes_pipeline, ['missed_bytes']),
        ('local', local_pipeline, ['local_orig', 'local_resp']),
        ('attack', numerical_pipeline, ['attack_rate', 'attack_amount']),
    ])

    return preprocessing

def transform_target(train: pd.DataFrame) -> pd.Series:
    # label
    class StringToBoolTransformer(BaseEstimator, TransformerMixin):    
        def __init__(self):
            pass
    
        def fit(self, X, y=None):
            return self
    
        def transform(self, X):
            X_transformed = X.copy()
            X_transformed = X_transformed.apply(lambda x: x != 'none')
            return X_transformed
    
        def get_feature_names_out(self, input_features=None):
            return input_features

    transformer = ColumnTransformer([
        ('label', StringToBoolTransformer(), ['label_tactic']),
    ])

    return transformer.fit_transform(train).reshape(-1,)

def kn_pipeline() -> Pipeline:
    param_grid = {
        'n_neighbors': [1, 3],
        'weights': ['uniform'],
        'algorithm': ['ball_tree', 'kd_tree'],
        'leaf_size': [15, 20],
    }

    grid = GridSearchCV(KNeighborsClassifier(),
                                param_grid=param_grid,
                                cv=5,
                                scoring='accuracy')
    
    full_pipeline = Pipeline(steps=[
        ('preprocessing', preprocessing_pipeline()),
        ('estimator', grid),
    ])

    return full_pipeline

def fit_kn_pipeline(path: str) -> None:
    # train a new Kn model and stores the pipeline

    train = pd.read_parquet(path, engine='pyarrow')
    pattern = obtain_categorical_pattern(train)
    print(pattern)
    pipeline_id = uuid.uuid4()
    
    pipeline = kn_pipeline()
    pipeline.fit(train, transform_target(train))

    print(f'best parameter groups: {pipeline.named_steps["estimator"].best_params_}')
    print(f'best score: {pipeline.named_steps["estimator"].best_score_}')

    pipeline_info = Pipeline_info(pipeline_id, pattern, pipeline.named_steps["estimator"].best_score_, pipeline.named_steps["estimator"].best_params_, pipeline)
    filename = f'kn_{pipeline_id}.pkl'
    with open(f'E:\\Group2Networked_AI_Systems_Project\\pipelines\\{filename}','wb') as file:
        pickle.dump(pipeline_info, file)

def rf_pipeline() -> Pipeline:
    param_grid = {
        'criterion':['gini'],
    }

    grid = GridSearchCV(RandomForestClassifier(),
                        param_grid=param_grid,
                        cv=5,
                        scoring='accuracy')
    
    full_pipeline = Pipeline(steps=[
        ('preprocessing', preprocessing_pipeline()),
        ('estimator', grid),
    ])

    return full_pipeline

def fit_rf_pipeline(path: str) -> None:
    # train a new Kn model and stores the pipeline

    train = pd.read_parquet(path, engine='pyarrow')
    pattern = obtain_categorical_pattern(train)
    print(pattern)
    pipeline_id = uuid.uuid4()
    
    pipeline = rf_pipeline()
    pipeline.fit(train, transform_target(train))

    print(f'best parameter groups: {pipeline.named_steps["estimator"].best_params_}')
    print(f'best score: {pipeline.named_steps["estimator"].best_score_}')

    pipeline_info = Pipeline_info(pipeline_id, pattern, pipeline.named_steps["estimator"].best_score_, pipeline.named_steps["estimator"].best_params_, pipeline)
    filename = f'rf_{pipeline_id}.pkl'
    with open(f'E:\\Group2Networked_AI_Systems_Project\\pipelines\\{filename}','wb') as file:
        pickle.dump(pipeline_info, file)

def ridge_pipeline() -> Pipeline:
    param_grid = {
        'alpha':[1.0],
        'tol': [1e-4, 1e-3]
    }

    grid = GridSearchCV(RidgeClassifier(),
                        param_grid=param_grid,
                        cv=5,
                        scoring='accuracy')
    
    full_pipeline = Pipeline(steps=[
        ('preprocessing', preprocessing_pipeline()),
        ('estimator', grid),
    ])

    return full_pipeline

def fit_ridge_pipeline(path: str) -> None:
    # train a new Kn model and stores the pipeline

    train = pd.read_parquet(path, engine='pyarrow')
    pattern = obtain_categorical_pattern(train)
    print(pattern)
    pipeline_id = uuid.uuid4()
    
    pipeline = ridge_pipeline()
    pipeline.fit(train, transform_target(train))

    print(f'best parameter groups: {pipeline.named_steps["estimator"].best_params_}')
    print(f'best score: {pipeline.named_steps["estimator"].best_score_}')

    pipeline_info = Pipeline_info(pipeline_id, pattern, pipeline.named_steps["estimator"].best_score_, pipeline.named_steps["estimator"].best_params_, pipeline)
    filename = f'ridge_{pipeline_id}.pkl'
    with open(f'E:\\Group2Networked_AI_Systems_Project\\pipelines\\{filename}','wb') as file:
        pickle.dump(pipeline_info, file)

def gb_pipeline() -> Pipeline:
    param_grid = {
        'learning_rate':[0.1],
    }

    grid = GridSearchCV(GradientBoostingClassifier(),
                        param_grid=param_grid,
                        cv=5,
                        scoring='accuracy')
    
    full_pipeline = Pipeline(steps=[
        ('preprocessing', preprocessing_pipeline()),
        ('estimator', grid),
    ])

    return full_pipeline

def fit_gb_pipeline(path: str) -> None:
    # train a new Kn model and stores the pipeline

    train = pd.read_parquet(path, engine='pyarrow')
    pattern = obtain_categorical_pattern(train)
    print(pattern)
    pipeline_id = uuid.uuid4()
    
    pipeline = gb_pipeline()
    pipeline.fit(train, transform_target(train))

    print(f'best parameter groups: {pipeline.named_steps["estimator"].best_params_}')
    print(f'best score: {pipeline.named_steps["estimator"].best_score_}')

    pipeline_info = Pipeline_info(pipeline_id, pattern, pipeline.named_steps["estimator"].best_score_, pipeline.named_steps["estimator"].best_params_, pipeline)
    filename = f'gb_{pipeline_id}.pkl'
    with open(f'E:\\Group2Networked_AI_Systems_Project\\pipelines\\{filename}','wb') as file:
        pickle.dump(pipeline_info, file)

def load_pipeline(path: str):
    with open(path, 'rb') as file:
        pipeline_info = pickle.load(file)
    return pipeline_info

def compare_similarity_accuracy(path: str, pipeline_info: Pipeline_info):
    train = pd.read_parquet(path, engine='pyarrow')
    pred = pipeline_info.pipeline.predict(train.drop(labels=['label_tactic'],axis=1))
    true_y = train['label_tactic']
    true_y = true_y.apply(lambda x: False if x == 'none' else True)
    counts = (pred==true_y).value_counts()
    accuracy = counts[True] / counts.sum()
    similarity = evaluate_categorical_similarity(pipeline_info.pattern, obtain_categorical_pattern(train))
    return accuracy, similarity

if __name__ == "__main__":
    test1 = pd.read_parquet('E:\\Group2Networked_AI_Systems_Project\\Data_sets\\test_set\\traffic_0.parquet')
    print(schedule(test1))
    #fit_gb_pipeline("E:\\Group2Networked_AI_Systems_Project\\Data_sets\\train_batch\\train_batch247.parquet")
"""
    my_pipeline_info = load_pipeline("E:\\Group2Networked_AI_Systems_Project\\pipelines\\gb_583a0552-b447-4794-bd88-ef1a81d77a96.pkl")
    try:
        a_lst, s_lst = [], []
        for i in chain(range(0,247), range(248,249)):
            a, s = compare_similarity_accuracy(f"E:\\Group2Networked_AI_Systems_Project\\Data_sets\\train_batch\\train_batch{i}.parquet", my_pipeline_info)
            a_lst.append(a)
            s_lst.append(s)
    except ValueError as e:
        print(f"Error occurs when processing {i}'th file, error is {e}")
    finally:
        plt.scatter(x=s_lst,y=a_lst)
        plt.title("Accuracy - Similarity")
        plt.xlabel("Cosine similarity")
        plt.ylabel("Accuracy of prediction made by GB 248")
        plt.savefig("E:\\Group2Networked_AI_Systems_Project\\figures\\sim_accuracy_gb_248.png")
        plt.show()
        plt.close()
#"""