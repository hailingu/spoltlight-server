# The data exchange protocol spotlight-server

spotlight-server accepts a job flow expressed in json format. The simplest job flow is shown under below.

    id: flow id. It is constructed from the project name, timestamp, the flow index in today flow sequence. 
    rid: running operator id. It is built from the flow id
    deps: the dependencies of the operator.

    {
        "flow": {
            "name": "simplest_flow",
            "id": "",
            "backend": "spark",
            "operators": [
                {
                    "name": "IrisMultiClassData",
                    "op-index": "job0",
                    "rid": "",
                    "op-category": "data-import",
                    "op-name": "import-csv",
                    "params": {
                        "input": "datasets/sample/iris_duplicated.txt",
                    },
                    "deps": [
                        
                    ]
                },
                {
                    "name": "RemoveDuplicatedRows",
                    "rid": "",
                    "op-index": "job1",
                    "op-category": "data-trasformation",
                    "op-name": "remove-duplicated-rows",
                    "params": {
                        "columns": 'ID SepalLength SepalWidth PetalLength PetalWidth Species'
                    },
                    "deps": [
                        "job0"
                    ]
                    
                },
                {
                    "name": "RandomForest",
                    "op-index": "job2",
                    "rid": "",
                    "params": {
                        "regulization": "",
                        "regulization_weight": 0,
                        "max_iteraion_num": 10
                    },
                    "deps": [
                        "job1"
                    ]
                }
            ]
        }
    }


    This is a default backend json Operation

  {
        "flow": {
            "name": "simplest_flow",
            "id": "",
            "backend": "scikitlearn",
            "operators": [
                {
                    "name": "IrisMultiClassData",
                    "op-index": "job0",
                    "rid": "",
                    "op-category": "data-import",
                    "op-name": "import-csv",
                    "params": {
                        "input": "datasets/sample/iris_duplicated.txt"
                    },
                    "deps": [
                        
                    ]
                },
                {
                    "name": "RemoveDuplicatedRows",
                    "rid": "",
                    "op-index": "job1",
                    "op-category": "data-trasformation",
                    "op-name": "remove-duplicated-rows",
                    "params": {
                        "columns": "'ID SepalLength SepalWidth PetalLength PetalWidth Species'"
                    },
                    "deps": [
                        {
                            "op-index": "job0",
                            "op-out-index": 0
                        }
                    ]
                },
                {
                   "name": "DataSplit",
                    "op-index": "job2",
                    "op-category": "sample",
                    "op-name": "data-split",
                    "rid": "",
                    "params": {
                        "percentage": 0.7
                    },
                    "deps": [
                        {
                            "op-index": "job1",
                            "op-out-index": 0
                        }
                    ]
                },
                {
                    "name": "Train",
                    "op-index": "job3",
                    "op-category": "machine-learning",
                    "op-name": "train",
                    "rid": "",
                    "params": {

                    },
                    "deps": [
                        {
                            "op-index": "job4",
                            "op-out-index": 0
                        },
                        {
                            "op-index": "job2",
                            "op-out-index": 0
                        }
                    ]
                },

                {
                    "name": "RandomForest",
                    "op-index": "job4",
                    "op-category": "machine-learning_model_classification",
                    "op-name": "random-forest",
                    "rid": "",
                    "params": {
                 
                    },
                    "deps": [

                    ]
                }
            ]
        }
    }
