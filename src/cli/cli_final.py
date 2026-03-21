
from src.database.database import SQLDatabase
class CLI:
    def __init__(self, db, entity_map,search_fields,allowed_fields, queries):
        self._db = db
        self._entity_map = entity_map
        self._search_fields = search_fields
        self._allowed_fields = allowed_fields
        self._queries = queries

    def run(self):
        print('Welcome to Zendesk Search')
        print("Type 'quit' to exit at any time, Press 'Enter' to continue")
        input()
        # users can use it multiple times even if they write the wrong format
        while True:
            print("\nSelect search options: ")
            print("* Press 1 to search Zendesk")
            print("* Press 2 to view a list of searchable fields")
            print("* Type 'quit' to exit")
            choice = input('> ')
            if choice == 'quit':
                break
            if choice not in ["1", "2"]:
                print("Invalid choice. Please enter 1,2 or 'quite'")
                continue
            if choice == "1":
                return self.search_zendesk()
            if choice == '2':
                return self.searchable_fields()

    def search_zendesk(self):
        print("\nSelect 1) Users or 2) Tickets or 3) Organizations")
        # consider there is 1 option will happen, what if customer enter 1) rather than 1 , also, what if they enter symbol, and what if they write down the string. that will be great.
        choice_num= input("> ").strip()
        if choice_num not in ["1" ,"2","3" ]:
            print('please enter 1,or 2, or 3')
            return
        table_name = self._entity_map[choice_num]
        print('Enter Search term')

        search_col = input("> ").strip()
        if search_col not in self._allowed_fields[table_name]:
            print(f"Invalid search field: {search_col}")
            print("Please choose a valid field.")
            return
        print('Enter search value')
        search_val = input("> ").strip()
        query = self._queries[table_name].format(search_col=search_col)
        query_results = self._db.read_query(query, params={"search_val": search_val})

        if query_results.empty:
            print("No results found")
            return
        prefix = ('users_', 'tickets_subject', 'organization_name')
        user_cols = [c for c in query_results.columns if c.startswith(prefix)]
        users_df = query_results[user_cols]

        # clean_users_df
        clean_users_df = users_df.groupby('users_id', as_index=False).agg(
            {
                "users_url": "first",
                "users_external_id": "first",
                "users_name": "first",
                "users_alias": "first",
                "users_created_at": "first",
                "users_active": "first",
                "users_verified": "first",
                "users_shared": "first",
                "users_locale": "first",
                "users_timezone": "first",
                "users_last_login_at": "first",
                "users_email": "first",
                "users_phone": "first",
                "users_signature": "first",
                "users_tag": "first",
                "users_suspended": "first",
                "organization_name": "first",
                "tickets_subject": list

            }
        )
        for index, row in clean_users_df.iterrows():
            print(row.to_string())


    def searchable_fields(self):
        print("\n-------------------------")
        print("Search Users with")
        for field in self._search_fields['users']:
            print(field)
        print("\n-------------------------")
        print("Search Tickets with")
        for field in self._search_fields['tickets']:
            print(field)
        print("\n-------------------------")


