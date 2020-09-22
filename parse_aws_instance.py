#!/usr/bin/python

import json

class AWSParsing:

    def get_controller(self, j):

        controller_id = ''
        # print(type(j))
        for instance in j['Reservations'][0]['Instances']:
            # print(instance['InstanceId'], instance['Tags'][0]['Value'])
            if instance['Tags'][0]['Value'] == 'controller':
                controller_id = instance['InstanceId']
        print(controller_id)
        return controller_id

    def get_controller_ip(self, j):


        controller_ip = ''
        # print(type(j))
        for instance in j['Reservations'][0]['Instances']:
            # print(instance['InstanceId'], instance['Tags'][0]['Value'])
            if instance['Tags'][0]['Value'] == 'controller':
                controller_ip = instance['PublicIpAddress']
        print(controller_ip)
        return controller_ip

    def main(self, command, aws_file):
        f = open(aws_file, 'rt')
        j = json.load(f)

        if command == 'gcont':
            return self.get_controller(j)
        elif command == 'gip':
            return self.get_controller_ip(j)
        else:
            return ''

if __name__ == '__main__':

    import sys
    command = str(sys.argv[1])
    file = str(sys.argv[2])
    ap = AWSParsing()
    ap.main(command, file)
