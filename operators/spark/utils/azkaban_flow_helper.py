import os

class AzkabanSparkOperator:

    def __init__(self, script_name, params, op_index, deps, output):
        self.name = str(op_index)
        self.op_type = 'command'
        self.params = params
        self.command = 'spark-submit --master local[2] '
        self.script_name = script_name
        self.deps = deps
        self.output = output
        return None

    def get_param(self):
        param = ' '
        
        for p in self.params:
            print(self.params[p])
            param = param + self.params[p] + ' '
        return param + self.output

    def get_deps(self, prefix_space):
        deps_str = ''
        for dep in self.deps:
            deps_str = prefix_space + '    - ' + dep + '\n'

        return deps_str

        
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
        self.file.write(prefix_space + '- name: ' + operator.name + '\n')
        self.file.write(prefix_space + '  type: ' + operator.op_type + '\n')
        self.file.write(prefix_space + '  config:' + '\n')
        self.file.write(prefix_space + '    command: ' + operator.command + operator.script_name + operator.get_param() + '\n')
        self.file.write(prefix_space + '  dependsOn: \n' + operator.get_deps(prefix_space) + '\n')

    def close(self):
        self.file.close()

    def __format_ctrl(self):
        i = 0
        prefix_space = ''
        while i < self.node_level:
            i += 1
            prefix_space += ' '
        
        return prefix_space
    

    