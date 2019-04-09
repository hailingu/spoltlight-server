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
                    "op-index": 0,
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
                    "op-index": 1,
                    "op-category": "data-trasformation",
                    "op-name": "remove-duplicated-rows",
                    "params": {
                        "columns": 'ID SepalLength SepalWidth PetalLength PetalWidth Species'
                    },
                    "deps": [
                        0
                    ]
                    
                },
                {
                    "name": "LinearRegression",
                    "op-index": 2,
                    "rid": "",
                    "params": {
                        "regulization": "",
                        "regulization_weight": 0,
                        "max_iteraion_num": 10
                    },
                    "deps": [
                        1
                    ]
                }
            ]
        }
    }




