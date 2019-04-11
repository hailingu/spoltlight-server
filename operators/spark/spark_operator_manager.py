class SparkOperatorManager:
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

        if op_name_lower == 'remove_duplicated_rows' or op_name_lower == 'remove-duplicated-rows' or op_name_lower == 'remove-duplicated-rows':
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

        if op_name_lower == 'randomforest' or op_name_lower == 'random_forest' or op_name_lower == 'random-forest':
            return RandomForest(json_param)


class SparkOperator:
    def __init__(self, json_param):
        self.script_location = None
        self.json_param = json_param


class ImportCSV(SparkOperator):
    def __init__(self, json_param):
        self.script_location = 'operators/spark/data_import/import_csv.py'
        self.json_param = json_param
        self.input_path = self.json_param['input']
        self.output_path = self.json_param['output']
        self.op_category = 'DataImport'

    def __str__(self):
        return 'import_csv.py ' + self.input_path + ' ' + self.output_path

    def to_string(self):
        return self.__str__()


class RemoveDuplicatedRows(SparkOperator):
    def __init__(self, json_param):
        self.script_location = 'operators/spark/data_transformation/remove_duplicated_rows.py'
        self.json_param = json_param
        self.input_path = self.json_param['input']
        self.columns = json_param['columns']
        self.output_path = self.json_param['output']
        self.op_category = 'DataTransformation'

    def __str__(self):
        return 'remove_duplicated_rows.py ' + self.input_path + ' ' + self.output_path + ' ' + self.columns

    def to_string(self):
        return self.__str__()


class RandomForest(SparkOperator):
    def __init__(self, json_param):
        self.script_location = 'operators/spark/machine_learning/random_forest.py'
        self.json_param = json_param
        self.input_path = self.json_param['input']
        self.op_category = 'MachineLearning'

    def __str__(self):
        return 'random_forest.py ' + self.input_path

    def to_string(self):
        return self.__str__()