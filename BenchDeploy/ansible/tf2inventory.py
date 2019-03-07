import json
import yaml


def group(nodes):
    hosts = {private: {'ansible_host': public} for private, public in nodes}
    return {'hosts': hosts}


with open('../terraform/terraform.tfstate') as f:
    data = json.load(f)['modules'][0]['outputs']

public_ips = data['public_ips']['value'].split(', ')
private_ips = data['private_ips']['value'].split(', ')

assert len(public_ips) >= 3
assert len(private_ips) >= 3

manager, bench, *workers = zip(private_ips, public_ips)
result = {'all': {
    'children': {
        'bench': group([bench]),
        'manager': group([manager]),
        'workers': group(workers)
    },
    'vars': {
        'ansible_user': 'ubuntu',
        'ansible_ssh_private_key_file': 'resources/admin-paris.pem',
        'ansible_python_interpreter': '/usr/bin/python3'
    }
}}

with open('resources/inventory.yml', 'w') as f:
    yaml.dump(result, f, default_flow_style=False)
