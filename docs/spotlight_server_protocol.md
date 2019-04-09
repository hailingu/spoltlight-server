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
                    "op_index": 0,
                    "rid": "",
                    "params": {
                        "input": "datasets/sample/iris.txt",
                        "file_format": "csv",
                        "header": "true"
                    },
                    "deps": [
                        
                    ]
                },
                {
                    "name": "RemoveDuplicatedRows",
                    "rid": "",
                    "op_index": 1,
                    "params": {
                        "selected_columns": [
                            
                        ]
                    },
                    "deps": [
                        0
                    ]
                    
                },
                {
                    "name": "LinearRegression",
                    "op_index": 2,
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




curl -k --get --data "session.id=223521ae-e92a-4fcc-a45a-bc6a4815c9d3&delete=true&project=test" http://localhost:8081/manager
curl -k -X POST --data "session.id=223521ae-e92a-4fcc-a45a-bc6a4815c9d3&name=test&description=test" http://localhost:8081/manager?action=create

curl -k -i -H "Content-Type: multipart/mixed" -X POST --form 'session.id=e6bb43ca-6daf-4dfe-a0a7-81d7a2bb2190' --form 'ajax=upload' --form 'file=@Archive.zip;type=application/zip' --form 'project=test;type/plain' http://localhost:8081/manager

