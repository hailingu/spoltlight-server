from opertor import Operator


class RemoveDuplicatedRows(Operator):
    def __init__(self, input, output, columns):
        self.operator_type = 'default scikit-learn method'
        self.operator_name = 'remove duplicated rows'
        self.data = None
        self.input = input
        self.output = output
        self.columns = columns

    def run(self):
        return None
    
