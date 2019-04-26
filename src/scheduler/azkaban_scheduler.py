import requests
import json
import os

from scheduler.scheduler import Scheduler
from operators.spark.spark_operator import SparkOperator
from utils.utils import mkdir
from operators.spark.spark_flow import SparkFlow

class AzkabanScheduler(Scheduler):
    '''This is the azkabanSchedule'''
    
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self.scheduler_pending_flows = {}
        self.scheduler_running_flows = {}
        self.scheduler_success_flows = {}
        self.scheduler_failded_flows = {}
        self.scheduler_azkaban_client = AzkabanClient()
    
    def submit(self, flow):
        self.scheduler_pending_flows[flow.flow_id] = flow
        self.scheduler_azkaban_client.create_project(flow.flow_id)
        self.__init__azkaban_flow__(flow.flow_id)

    def get_status(self, flow_id):
        pass

    def run(self, flow_id):
        self.scheduler_azkaban_client.execute_flow(flow_id, flow_id)
        return None
        
    def pause(self, flow_id):
        pass

    def stop(self, flow_id):
        pass

    def shutdown(self):
        pass

    def __init__azkaban_flow__(self, flow_id):
        if not flow_id in self.scheduler_pending_flows:
            return 

        flow = self.scheduler_pending_flows[flow_id]
        mkdir(flow.flow_working_directory)
        mkdir(flow.flow_working_directory + 'azkaban/')
        mkdir(flow.flow_working_directory + 'output/')
        os.system('cp resources/azkaban_job_template/flow20.project ' + flow.flow_working_directory + 'azkaban/')

        azkabanFlowReaderAndWriterHelper = AzkabanFlowReadAndWriteHelper(flow.flow_working_directory + 'azkaban/' + flow.flow_id + '.flow')
        for op_name in flow.flow_pending_operators:
            os.system('cp ' +  flow.flow_pending_operators[op_name].op_script_location + ' ' + flow.flow_working_directory + '/azkaban/')
            azkabanFlowReaderAndWriterHelper.write(flow.flow_pending_operators[op_name])

        azkabanFlowReaderAndWriterHelper.close()
        os.system('cd ' + flow.flow_working_directory + '/azkaban/&&zip -r ' + flow.flow_id + '.zip .&&mv ' + flow.flow_id + '.zip ..')
        self.scheduler_azkaban_client.login()
        self.scheduler_azkaban_client.upload(flow.flow_working_directory + '/' + flow.flow_id, flow.flow_id)        
       
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


class AzkabanNodeHelper:

    def __init__(self, operator):
        self.node_name = str(operator['op-index'])
        self.node_type = 'command'
        self.node_operator = operator 
        self.node_command = operator.azkaban_script()
        self.node_deps = operator.op_json_param['deps'] if len(operator.op_json_param['deps']) > 0 else None

    def get_deps(self, prefix_space):
        deps_str = ''
        if self.node_deps == None:
            return deps_str
        
        for dep in self.node_deps:
            deps_str = prefix_space + '    - ' + dep['op-index'] + '\n'

        return deps_str

    def to_string(self, prefix_space):
        op_str = prefix_space + '- name: ' + self.node_name + '\n'
        op_str = op_str + prefix_space + '  type: ' + self.node_type + '\n'
        op_str = op_str + prefix_space + '  config: ' + '\n'
        op_str = op_str + prefix_space + '    command: ' + self.node_command + '\n'
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

