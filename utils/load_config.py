import yaml
from pathlib import Path

#{'base_path': 'src/data_source/',
# 'data_source': ['organizations.json', 'tickets.json', 'users.json'],
# 'compulsory_rules': {
#                       'tickets.json': ['_id', 'url', 'external_id', 'submitter_id', 'assignee_id', 'organization_id'],
#                       'users.json': ['_id', 'external_id', 'organization_id'],
#                       'organizations.json': ['_id', 'external_id']},
# 'schema_rules': {'
#                       tickets.json': ['_id', 'url', 'external_id', 'created_at', 'type', 'subject', 'description', 'priority', 'status', 'submitter_id', 'assignee_id', 'organization_id', 'tags', 'has_incidents', 'due_at', 'via'],
#                       'users.json': ['_id', 'external_id', 'name', 'alias', 'created_at', 'active', 'verified', 'shared', 'locale', 'timezone', 'last_login_at', 'email', 'phone', 'signature', 'organization_id', 'tags', 'suspended', 'role'],
#                       'organizations.json': ['_id', 'url', 'external_id', 'name', 'domain_name', 'created_at', 'details', 'shared_tickets', 'tags']},
# 'database': {'url': 'sqlite:///zendesk.db'}}


def load_conf():
    # This function is used to load the config file.
    current_file = Path(__file__)
    current_proejct_abs = Path(__file__).parent.parent.resolve()
    config_path = current_proejct_abs / 'config.yml'
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


