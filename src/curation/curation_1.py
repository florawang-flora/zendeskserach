import pandas as pd
import yaml
class Curation:
    # step1: check the dataframe format
    # step2: apply the business logic to dataframes
    # step3: generate database format

    def __init__(self, file_names, dict_of_dataframe):
        self._file_names = file_names
        self._dict_of_dataframe = dict_of_dataframe

    @property
    def get_file_names(self):
        return self._file_names
    @property
    def get_dict_of_dataframes(self):
        return self.get_dict_of_dataframes

    def _check_dataframes_format(self):
        # check the dataframe exists and types
        # check for the gen
        for file_name in self._file_names:
            df = self._dict_of_dataframe.get(file_name)
            if df is None:
                raise ValueError(f'Missing dataframe: {file_name}')
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f'{file_name} is not a Data frame, got {type(df)}')
            # check the primary keys
            self._check_primary_key(df,"_id", file_name)

    def _check_primary_key(self, df, pk_column, file_name):
        """
        Primary key rules: id columns
        - whether this column exits
        - no null value
        - make sure it's unique
        """
        if pk_column not in df.columns:
            raise ValueError(f"Missing {pk_column} primary columns in {file_name}")

        if df[pk_column].isna().any():
            raise ValueError(f'{pk_column} has null values in {file_name}')

        if not df[pk_column].is_unique:
            raise ValueError(f'{pk_column} is not unique in{file_name}')

        else:
            print(f'Primary Key Check ALL Passes in {file_name}!')

    def _apply_business_logic(self):
        """
         do the merge
         tickets.submitter_id -> users._id
        tickets.organization_id -> orgs._id

        """
        tickets_df = self._dict_of_dataframe.get('tickets.json')
        users_df = self._dict_of_dataframe.get('users.json')
        orgs_df = self._dict_of_dataframe.get('organizations.json')

        tickets_df = tickets_df.rename(columns = {
            "_id": "tickets_id",
            "url": "tickets_url",
            "external_id": "tickets_external_id",
            "created_at": "tickets_created_at",
            "type": "tickets_type",
            "subject": "tickets_subject",
            "description": "tickets_description",
            "priority": "tickets_priority",
            "status": "tickets_status",
            "submitter_id": "tickets_submitter_id",
            "assignee_id": "tickets_assignee_id",
            "organization_id": "tickets_organization_id",
            "tags": "tickets_tags",
            "has_incidents": "tickets_has_incidents",
            "due_at": "tickets_due_at",
            "via": "tickets_via"
                }
        )

        users_df = users_df.rename( columns = {
            "_id": "users_id",
            "url": "users_url",
            "external_id": "users_external_id",
            "name": "users_name",
            "alias": "users_alias",
            "created_at": "users_created_at",
            "active": "users_active",
            "verified": "users_verified",
            "shared": "users_shared",
            "locale": "users_locale",
            "timezone": "users_timezone",
            "last_login_at": "users_last_login_at",
            "email": "users_email",
            "phone": "users_phone",
            "signature": "users_signature",
            "organization_id": "organization_id",
            "tags": "users_tag",
            "suspended": "users_suspended",
            "role": "users_role"
            }
        )
        #print(f"users df {users_df.head()}")

        orgs_df = orgs_df.rename(columns = {
            "_id": "organization_id",
            "url": "org_url",
            "external_id": "org_external_id",
            "name": "organization_name",
            "domain_names":"org_domain",
            "created_at": "org_created_at",
            "details": "org_details",
            "shared_tickets": "org_shared_tickets",
            "tags": "org_tags"
            }
        )
        #print(orgs_df.head())
        # change their.
        merge_df = users_df.merge(tickets_df.rename(
            columns= {'tickets_submitter_id': "users_id"}
        ),
            how= 'left',
            on = 'users_id')

        merge_df2 = merge_df.merge(orgs_df,
                                   how ='left',
                                   on = 'organization_id')

        print(f" here it is columns in the database format{list(merge_df2.columns)}")

        return merge_df2
    def generate_database_format(self):
        # steps: check the dataframes format and apply the business logic
        self._check_dataframes_format()
        # generate_database_format
        self._apply_business_logic()
        return self._apply_business_logic()