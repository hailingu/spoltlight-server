import requests
import json
import os


from scheduler.scheduler import Scheduler

class AzkabanScheduler(Scheduler):
    '''This is the azkabanSchedule'''
    
    pass


class AzkabanClient:
    def __init__(self):
        self.session_id = None
        self.url = 'http://localhost:8081'
        self.upload_url = 'curl -i -X POST --cookie "azkaban.browser.session.id={}" --form "project={}" --form "action=upload" --form "file=@{}.zip;type=application/zip" http://localhost:8081/manager'
        self.create_url = 'curl -k -X POST --data "session.id={}&name={}&description={}" http://localhost:8081/manager?action=create'
        self.delete_url = 'curl -k --get --data "session.id={}&delete=true&project={}" http://localhost:8081/manager'
        self.execute_flow_url = 'curl -k --get --cookie "azkaban.browser.session.id={}" --data "ajax=executeFlow" --data "project={}" --data "flow={}" http://localhost:8081/executor'

    def login(self):
        data = 'action=login&username=azkaban&password=azkaban'
        headers = {
            'Content-type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest'
        }

        response = requests.post(self.url, data=data, headers=headers)
        response_json = json.loads(response.content)
        self.session_id = response_json['session.id']
        print(self.session_id)


    def create_project(self, project_name, description=''):
        formated_create_url = self.create_url.format(self.session_id, project_name, description)
        os.system(formated_create_url)


    def delete_project(self, project_name):
        formated_delete_url = self.delete_url.format(self.session_id, project_name)
        os.system(formated_delete_url)
        

    def upload(self, file_name, project_id):
        formated_upload_url = self.upload_url.format(self.session_id, project_id, file_name)
        os.system(formated_upload_url)        
        
    def execute_flow(self, project_id, flow_id):
        format_execute_flow = self.execute_flow_url.format(self.session_id, project_id, flow_id)
        os.system(format_execute_flow)

azkabanClient = AzkabanClient()


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