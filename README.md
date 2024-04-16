# Group 2 Project Repository

Structure of the project:
1. Data_sets: contains training data (parquet format). To load the training set, use ```pd.read_parquet({path}, engine='pyarrow')``` function.

2. figures: collections of images, each presenting the accuracy-cosine_similarity relationship for a classifier. The name of the classifier is included in the filename of the image. For example, the first image ```sim_accuracy_gb_118.png``` represents Gradient boost classifier with ID 118. Human inspection is required in recognizing the relationship.

3. models: ignore this one as it is not used in deployment.

4. root
    - test_set: collections of test sets. Use ```pd.read_parquet()``` method for loading.
    - enter.py: the entry of the root node. To activate the root, run ```python enter.py <batch_path>``` in cmd. The main function schedule and assign the task to an available worker node by establishing a TCP connection.
    - metric.db: collection of the metrics of pipelines on worker side. Task assignment is based on those metrics. the schema of the database is: ```index: int | PID: str | Pattern: str | ht: float64 | lt: float64 | cid: int |```. 
    - pipeline_metric_manage.py: collection of methods used for managing and accessing  ```metric.db```. 
    - schedule.py: collection of methods used for scheduling tasks. The schedule is based on functions in ```pipeline_metric_manage.py``` and ```similary_calculation.py```. A new dict object will be created representing a new task. 
    - similary_calculation.py: collection of methods to evaluate the similarity between an incoming batch and previous batches.

5. worker
    - pipeline: collection of pre-trained pipelines stored in ```pkl``` format. Use ```pickle.load(filename).pipeline``` to access the pipeline.
    - data.pkl: serialized data for testing usage.
    - enter.py: the entry of the worker node. To activate the worker, run ```python enter.py``` to load the file sent from the root. It calls methods in ```task.py``` to process and analyze the incoming batches.
    - pipeline.py: collections of methods used in training and encapsulating the pipelines. Not involved in deployment phase.
    - task.py: collection of methods used for detecting anomalies in the incoming batches.

## Usage

### Requirements

- docker
- docker-compose

To run a cluster first clone the repository

```git clone https://github.com/LukaAlhonen/Group2Networked_AI_Systems_Project.git```

#### Root

```cd Group2Networked_AI_Systems_Project/root```

```export ROOT_PORT=<port for root>```

```docker-compose up```

or

```sudo -E docker-compose up```

#### Worker

```cd Group2Networked_AI_Systems_Project/worker```

```export ROOT_ADDRESS=<ip address of root machine>```

```export ROOT_PORT=<port for root>```

```export WORKER_PORT=<port for worker>```

```docker-compose up```

or

```sudo -E docker-compose up```