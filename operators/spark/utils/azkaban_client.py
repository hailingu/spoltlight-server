import requests
import json
import os

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
