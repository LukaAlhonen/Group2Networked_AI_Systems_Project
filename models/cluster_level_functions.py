import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import shelve

class NetworkPair:
    # Each NetworkPair instance record previous attacking informtion related to a specific connection

    def __init__(self, amount: int, attack_amount: int, comm_id: str) -> None:
        self.amount = amount                # Total amount of previous traffic 
        self.attack_amount= attack_amount   # Total amount of previous malicious traffic
        self.comm_id = comm_id              # Calculated from: hash(proto, src_ip, src_port, dest_ip, dest_port)

def add_attack_history(df: pd.DataFrame, path: str) -> pd.DataFrame:
    # add attack_amount and attack_rate to data, according to community_id. Return aggregated dataframe.

    df['attack_rate'] = pd.Series(dtype=float)
    df['attack_amount'] = pd.Series(dtype=int)
    
    with shelve.open(path, flag='r') as db:
        for index, row in df.iterrows():
            comm_id = row['community_id']
            if comm_id in db:
                df.at[index, 'attack_rate'] = db[comm_id].attack_amount / db[comm_id].amount
                df.at[index, 'attack_amount'] = db[comm_id].attack_amount
            else:
                df.at[index, 'attack_rate'] = 0.
                df.at[index, 'attack_amount'] = 0
        db.close()

    return df

def update_shelve(df: pd.DataFrame, path: str) -> None:
    # Update only info of malicious traffic. pred should has at least columns 'proto', 'src_ip', 'dest_ip', 'src_port', 'dest_port', 'community_id' and 'label_tactic'
    
    connections = df.groupby('community_id')['label_tactic'].value_counts().unstack(fill_value=0)
    connections.rename(columns={True:"True_count", False:"False_count"}, inplace=True)
    connections = connections[connections["True_count"]!=0]

    with shelve.open(path, flag='c') as db:
        for index, row in connections.iterrows():
            if index in db:
                db[index].attack_amount += row['True_count']
                db[index].amount += (row['True_count'] + row['False_count'])
            else:
                network_pair = NetworkPair(row['True_count']+row['False_count'], row['True_count'], index)
                db[index] = network_pair
        db.close()