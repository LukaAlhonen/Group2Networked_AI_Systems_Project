import pickle 
import pandas as pd

def task_deserialize(serialized_data):
    task = pickle.loads(serialized_data)
    return task

def task_process(task, path='E:\\Group2Networked_AI_Systems_Project\\worker\\pipelines'):
    batch = task['batch']
    pids = task['pid']
    pred = []
    for pid in pids:
        ppath = path + f"\\{pid}"
        with open(ppath,'rb') as f:
            p_info = f.read()
        pipeline_info = pickle.loads(p_info)
        model = pipeline_info.pipeline
        pred_pid = model.predict(batch)
        pred.append(pred_pid)
    return pred, batch

def task_analysis(pred, batch):
    # Result consists: 
    l = len(pred)
    if l == 3:
        # Transforming True to 1 and False to 0
        transformed_data = [[int(val) for val in sublist] for sublist in pred]

        # Calculating the average for each column
        num_rows = len(transformed_data)
        num_cols = len(transformed_data[0])

        column_sums = [sum(transformed_data[j][i] for j in range(num_rows)) for i in range(num_cols)]
        column_averages = [round(sum_ / num_rows, 2) for sum_ in column_sums]

        df = pd.DataFrame(column_averages)
        df['Label'] = batch['label_tactic'].values
        df['Label'] = df['Label'].apply(lambda x: False if x == 'none' else True)

        metrics = {}
        df['pred'] = df[0] >= 0.5
        overall_voting_accuracy = (df['pred'] == df['Label']).mean()
        true_positive = ((df['pred'] == True) & (df['Label'] == True)).sum() / (df['Label'] == True).sum()
        true_negative = ((df['pred'] == False) & (df['Label'] == False)).sum() / (df['Label'] == False).sum()
        false_positive = ((df['pred'] == True) & (df['Label'] == False)).sum()
        false_negative = ((df['pred'] == False) & (df['Label'] == True)).sum()
        recall = true_positive / (true_positive + false_negative)
        precision = true_positive / (true_positive + false_positive)
        specificity = true_negative / (true_negative + false_positive)
        metrics['overall_accuracy'] = overall_voting_accuracy
        metrics['true_positive'] = true_positive
        metrics['true_negative'] = true_negative
        metrics['recall'] = recall
        metrics['precision'] = precision
        metrics['specificity'] = specificity
        # for 0.67 and 0.33, we also need to summarize
        divergence_rate = ((df[0]!=0) & (df[0]!=1)).sum() / (df[0]).sum()
        metrics['divergence_rate'] = divergence_rate
        right_decision_rate = (((df[0]<1) & (df[0]>0.5) & (df['Label']==True)).sum() + ((df[0]<0.5) & (df[0]>0) & (df['Label']==False)).sum()) / ((df[0]!=0) & (df[0]!=1)).sum()
        metrics['right_decision_rate'] = right_decision_rate
        return metrics
    if l == 7:
        # Transforming True to 1 and False to 0
        transformed_data = [[int(val) for val in sublist] for sublist in pred]

        # Calculating the average for each column
        num_rows = len(transformed_data)
        num_cols = len(transformed_data[0])

        column_sums = [sum(transformed_data[j][i] for j in range(num_rows)) for i in range(num_cols)]
        column_averages = [round(sum_ / num_rows, 2) for sum_ in column_sums]

        df = pd.DataFrame(column_averages)
        df['Label'] = batch['label_tactic'].values
        df['Label'] = df['Label'].apply(lambda x: False if x == 'none' else True)

        metrics = {}
        df['pred'] = df[0] >= 0.5
        overall_voting_accuracy = (df['pred'] == df['Label']).mean()
        true_positive = ((df['pred'] == True) & (df['Label'] == True)).sum() / (df['Label'] == True).sum()
        true_negative = ((df['pred'] == False) & (df['Label'] == False)).sum() / (df['Label'] == False).sum()
        false_positive = ((df['pred'] == True) & (df['Label'] == False)).sum()
        false_negative = ((df['pred'] == False) & (df['Label'] == True)).sum()
        recall = true_positive / (true_positive + false_negative)
        precision = true_positive / (true_positive + false_positive)
        specificity = true_negative / (true_negative + false_positive)
        metrics['overall_accuracy'] = overall_voting_accuracy
        metrics['true_positive'] = true_positive
        metrics['true_negative'] = true_negative
        metrics['recall'] = recall
        metrics['precision'] = precision
        metrics['specificity'] = specificity
        # for 0.67 and 0.33, we also need to summarize
        divergence_rate = ((df[0]!=0) & (df[0]!=1)).sum() / (df[0]).sum()
        metrics['divergence_rate'] = divergence_rate
        right_decision_rate = (((df[0]<1) & (df[0]>0.5) & (df['Label']==True)).sum() + ((df[0]<0.5) & (df[0]>0) & (df['Label']==False)).sum()) / ((df[0]!=0) & (df[0]!=1)).sum()
        metrics['right_decision_rate'] = right_decision_rate
        return metrics
            