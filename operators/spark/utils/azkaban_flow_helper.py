import os
from operators.spark.spark_operator_manager import SparkOperatorManager

class AzkabanSparkOperator:

    def __init__(self, operator, flow_id):
        self.name = str(operator['op-index'])
        self.op_type = 'command'
        self.spark_operator = SparkOperatorManager.get_operator(operator['op-name'], operator['op-category'], operator['params'])
        self.command = 'spark-submit --master local[2] '
        self.deps = operator['deps'] if len(operator['deps']) > 0 else None
        self.flow_id = flow_id


    def get_deps(self, prefix_space):
        deps_str = ''
        if self.deps == None:
            return deps_str
        
        for dep in self.deps:
            deps_str = prefix_space + '    - ' + dep + '\n'

        return deps_str


    def to_string(self, prefix_space):
        op_str = prefix_space + '- name: ' + self.name + '\n'
        op_str = op_str + prefix_space + '  type: ' + self.op_type + '\n'
        op_str = op_str + prefix_space + '  config: ' + '\n'
        op_str = op_str + prefix_space + '    command: ' + self.command + self.spark_operator.to_string() + '\n'
        op_str = op_str + prefix_space + '  dependsOn: \n' + self.get_deps(prefix_space) + '\n'
        return op_str

    def __str__(self):
        return self.to_string('')
        

class AzkabanFlowReadAndWriteHelper:
    def __init__(self, path):
        self.path = path
        self.file = None
        self.node_level = 0
        if not os.path.exists(self.path):
            self.file = open(self.path, 'w+')
            self.__header_info()
        else:
            self.file = open(self.path, 'w+')

    def __header_info(self):
        self.file.write('nodes:' + '\n')

    def open(self):
        if self.file == None:
            self.file = open(self.path, 'w+')

    def write(self, operator):
        self.open()
        prefix_space = self.__format_ctrl()
        self.file.write(operator.to_string(prefix_space))

    def close(self):
        self.file.close()

    def __format_ctrl(self):
        i = 0
        prefix_space = ''
        while i < self.node_level:
            i += 1
            prefix_space += ' '
        
        return prefix_space
    

    