

class CLI:
    def __init__(self,db):
        self.db = db
        self._tables = {
            "1" : ('Users', "user_cli"),
            "2" : ('Tickets', 'ticket_cli'),
            "3" : ('Organizations','organization_cli')
        }

    def welcome(self):
        print('\nWelcome to Zendesk Search')
        print("Type'quit' to execute at any time, press 'Enter' to continue")
        input()
        while True:
            print('\nSelect search options:')
            print('* Press 1 to search Zendesk')
            #print('* Press 2 to view a list of searchable fields')
            print("Type 'quit' to exit")
            # this is to help whether enter anything, clean the structure.
            choice= input("> ").strip().lower()
            if choice == 'quit':
                print('Bye!')
            if choice =='1':
                self.search_zendesk

    def search_zendesk(self):
        print('\nSelect 1) Users or 2) Tickets or 3) Organizations')
        entry_choice = intut("> ").strip().lower()
        if entry_choice == 'quit':
            return
        label, table = self._tables[entry_choice]
        print("\nEnter search term")
        # stop here/
        #column_name =
        #sql =



    def search_cli_ticket(self, ticket_id):
        ticket_sql = 'select * from ticket_cli where ticket_id = ‘’'
    def