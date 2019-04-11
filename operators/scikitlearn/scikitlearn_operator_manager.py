import pandas as pd
from operators.operator_status import OperatorStatus
from sklearn.ensemble import RandomForestClassifier

class ScikitlearnOperatorManager:
    def __init__(self):
        return None
    
    @staticmethod
    def get_operator(op_name, op_category, json_param):
        op_category_lower = op_category.lower()
        op = None
        if op_category_lower == 'data_import' or op_category_lower == 'dataimport' or op_category_lower == 'data-import':
            op = DataImportManager.get_operator(op_name, json_param)
        elif op_category_lower == 'datatransformation' or op_category_lower == 'data_transformation' or op_category_lower == 'data-transformation':
            op = DataTransformationManager.get_operator(op_name, json_param)
        elif op_category_lower == 'machinelearning' or op_category_lower == 'machine_learning' or op_category_lower == 'machine-learning':
            op = MachineLearningMananger.get_operator(op_name, json_param)
        return op

class DataImportManager:
    def __init__(self):
        return None

    @staticmethod
    def get_operator(op_name, json_param):
        op_name_lower = op_name.lower()
        if op_name_lower == None:
            return None

        if op_name == 'import_csv' or op_name == 'importcsv' or op_name == 'import-csv':
            return ImportCSV(json_param)

        return None


class DataTransformationManager:
    def __init__(self):
        return None

    @staticmethod
    def get_operator(op_name, json_param):
        op_name_lower = op_name.lower()
        if op_name_lower == None:
            return None

        if op_name_lower == 'remove_duplicated_rows' or op_name_lower == 'removeduplicatedrows' or op_name_lower == 'remove-duplicated-rows':
            return RemoveDuplicatedRows(json_param)
    
        return None


class MachineLearningMananger:
    def __init__(self):
        return None

    @staticmethod
    def get_operator(op_name, json_param):
        op_name_lower = op_name.lower()
        if op_name_lower == None:
            return None

        if op_name_lower == 'random_forest' or op_name_lower == 'randomforest' or op_name_lower == 'random-forest':
            return RandomForest(json_param)


class ScikitlearnOperator:
    def __init__(self):
        self.input_path = None
        self.result = None
        self.status = None
        self.op_name = None
        self.op_category = None

    def run(self):
        return self.status
    
    def get_result(self):
        return self.result


class ImportCSV(ScikitlearnOperator):
    def __init__(self, json_param):
        self.json_param = json_param
        self.input_path = self.json_param['input']
        self.result = None 
        self.op_category = 'DataImport'
        self.op_name = 'ImportCSV'

    def run(self):
        print(self)
        try:
            self.result = pd.read_csv(self.input_path, header=0, sep=',')
            self.status = OperatorStatus.SUCCESS
        except Exception as e:
            print('Exception ' + str(e))
            self.status = OperatorStatus.FAILED
        return self.status

    def __str__(self):
        return 'ImportCSV Operator ' + self.input_path

    def to_string(self):
        return self.__str__()


class RemoveDuplicatedRows(ScikitlearnOperator):
    def __init__(self, json_param):
        self.json_param = json_param
        self.input_op = self.json_param['input_op']
        self.columns = json_param['columns']
        self.result = None
        self.op_category = 'DataTransformation'
        self.op_name = 'RemoveDuplicatedRows'

    def run(self):
        print(self)
        try:
            data = self.input_op[0].get_result()
            data.drop_duplicates(subset=self.columns.strip("'").split(' '), inplace=True)
            self.result = data
            self.status = OperatorStatus.SUCCESS
        except Exception as e:
            print('Exception ' + str(e))
            self.status = OperatorStatus.FAILED
        return self.status
    

    def __str__(self):
        return 'RemoveDuplicatedRows ' + self.input_op[0].op_name + ' ' + self.columns

    def to_string(self):
        return self.__str__()


class RandomForest(ScikitlearnOperator):
    def __init__(self, json_param):
        self.json_param = json_param
        self.input_op = self.json_param['input_op']
        self.result = None
        self.op_category = 'MachineLearning'
        self.op_name = 'RandomForest'

    def run(self):
        print(self)
        try:
            data = self.input_op[0].get_result()
            cols = ['SepalLength', 'SepalWidth', 'PetalLength', 'PetalWidth']
            x = pd.DataFrame(data, columns = cols)
            Y = data['Species']

            self.result = RandomForestClassifier(oob_score=True, random_state=10)
            self.result.fit(x, Y)
            print(self.result.oob_score_)
            self.status = OperatorStatus.SUCCESS
        except Exception as e:
            print('Exception ' + str(e))
            self.status = OperatorStatus.FAILED
        return self.status


    def __str__(self):
        return 'RandomForest ' + self.input_op[0].op_name

    def to_string(self):
        return self.__str__()