# code for cli.
if table_name == 'users':
    query = f"""
                SELECT 
                    u._id as users_id,
                    u.url as users_url,
                    u.external_id as users_external_id,
                    u.name as users_name,
                    u.alias as users_alias,
                    u.created_at as users_created_at,
                    u.active as users_active,
                    u.verified as users_verified,
                    u.shared as users_shared,
                    u.locale as users_locale,
                    u.timezone as users_timezone,
                    u.last_login_at as users_last_login_at,
                    u.email as users_email,
                    u.phone as users_phone,
                    u.signature as users_signature,
                    u.organization_id as organization_id,
                    u.tags as users_tag,
                    u.suspended as users_suspended,
                    u.role as users_role,

                    t._id as tickets_id,
                    t.url as tickets_url,
                    t.external_id as tickets_external_id,
                    t.created_at as tickets_created_at,
                    t.type as tickets_type,
                    t.subject as tickets_subject,
                    t.description as tickets_description,
                    t.priority as tickets_priority,
                    t.status as tickets_status,
                    t.submitter_id as tickets_submitter_id,
                    t.assignee_id as tickets_assignee_id,
                    t.organization_id as tickets_organization_id,
                    t.tags as tickets_tags,
                    t.has_incidents as tickets_has_incidents,
                    t.due_at as tickets_due_at,
                    t.via as tickets_via,

                    o._id as organization_id,
                    o.url as org_url,
                    o.external_id as org_external_id,
                    o.name as organization_name,
                    o.domain_names as org_domain,
                    o.created_at as org_created_at,
                    o.details as org_details,
                    o.shared_tickets as org_shared_tickets,
                    o.tags as org_tags

                FROM users AS u 
                LEFT JOIN tickets AS t
                ON u._id = t.submitter_id 
                LEFT JOIN organizations AS o 
                ON o._id = u.organization_id
                WHERE u.{search_col} = :search_val;
           """
    query_results = self._db.read_query(self._queries[table_name], params={"search_val": search_val})
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
            "users_role": "first",
            "organization_name": "first",
            "tickets_subject": list

        }
    )
    for index, row in clean_users_df.iterrows():
        print(row.to_string())
    # print(clean_users_df)


elif table_name == 'tickets':
    query = f"""
         SELECT 
           t._id as tickets_id,
           t.url as tickets_url,
           t.external_id as tickets_external_id,
           t.created_at as tickets_created_at,
           t.type as tickets_type,
           t.subject as tickets_subject,
           t.description as tickets_description,
           t.priority as tickets_priority,
           t.status as tickets_status,
           t.submitter_id as tickets_submitter_id,
           t.assignee_id as tickets_assignee_id,
           t.organization_id as tickets_organization_id,
           t.tags as tickets_tags,
           t.has_incidents as tickets_has_incidents,
           t.due_at as tickets_due_at,
           t.via as tickets_via,

           u._id as users_id,
           u.url as users_url,
           u.external_id as users_external_id,
           u.name as users_name,
           u.alias as users_alias,
           u.created_at as users_created_at,
           u.active as users_active,
           u.verified as users_verified,
           u.shared as users_shared,
           u.locale as users_locale,
           u.timezone as users_timezone,
           u.last_login_at as users_last_login_at,
           u.email as users_email,
           u.phone as users_phone,
           u.signature as users_signature,
           u.organization_id as organization_id,
           u.tags as users_tag,
           u.suspended as users_suspended,
           u.role as users_role,

           o._id as organization_id,
           o.url as org_url,
           o.external_id as org_external_id,
           o.name as organization_name,
           o.domain_names as org_domain,
           o.created_at as org_created_at,
           o.details as org_details,
           o.shared_tickets as org_shared_tickets,
           o.tags as org_tags
        FROM tickets as t
        LEFT JOIN users AS u
        ON t.submitter_id = u._id
        LEFT JOIN organizations as o
        ON o._id = u.organization_id 
        WHERE t.{search_col} = :search_val
        """
    query_results = self._db.read_query(query, params={"search_val": search_val})

    prefix = ('tickets_', 'users_name', 'organization_name')
    ticket_cols = [c for c in query_results.columns if c.startswith(prefix)]
    ticket_df = query_results[user_cols]

    # clean_users_df
    clean_ticket_df = users_df.groupby('ticket_id', as_index=False).agg(
        {
            "tickets_url": "first",
            "tickets_external_id": "first",
            "tickets_created_at": "first",
            "tickets_type": "first",
            "tickets_subject": "first",
            "tickets_description": "first",
            "tickets_priority": "first",
            "tickets_status": "first",
            "tickets_submitter_id": 'first',
            "tickets_assignee_id": "first",
            "tickets_organization_id": "first",
            "tickets_tags": "first",
            "tickets_has_incidents": "first",
            "ickets_due_at": "first",
            "tickets_via": "first"

        }
    )
    for index, row in clean_ticket_df.iterrows():
        print(row.to_string())
    # print(clean_users_df)
elif table_name == 'organizations':
    query = f"""
        SELECT 
           o._id as organization_id,
           o.url as org_url,
           o.external_id as org_external_id,
           o.name as organization_name,
           o.domain_names as org_domain,
           o.created_at as org_created_at,
           o.details as org_details,
           o.shared_tickets as org_shared_tickets,
           o.tags as org_tags,

           u._id as users_id,
           u.url as users_url,
           u.external_id as users_external_id,
           u.name as users_name,
           u.alias as users_alias,
           u.created_at as users_created_at,
           u.active as users_active,
           u.verified as users_verified,
           u.shared as users_shared,
           u.locale as users_locale,
           u.timezone as users_timezone,
           u.last_login_at as users_last_login_at,
           u.email as users_email,
           u.phone as users_phone,
           u.signature as users_signature,
           u.organization_id as organization_id,
           u.tags as users_tag,
           u.suspended as users_suspended,
           u.role as users_role,

           t._id as tickets_id,
           t.url as tickets_url,
           t.external_id as tickets_external_id,
           t.created_at as tickets_created_at,
           t.type as tickets_type,
           t.subject as tickets_subject,
           t.description as tickets_description,
           t.priority as tickets_priority,
           t.status as tickets_status,
           t.submitter_id as tickets_submitter_id,
           t.assignee_id as tickets_assignee_id,
           t.organization_id as tickets_organization_id,
           t.tags as tickets_tags,
           t.has_incidents as tickets_has_incidents,
           t.due_at as tickets_due_at,
           t.via as tickets_via

        FROM organizations AS o
        LEFT JOIN users AS u
        ON u.organization_id = o._id
        LEFT JOIN tickets AS t
        ON t.submitter_id = u._id
        WHERE o.{search_col} = :search_val
        """
    # dataframe
    query_results = self._db.read_query(query, params={"search_val": search_val})
    prefix = ('users_', 'tickets_subject', 'organization_name')
    user_cols = [c for c in query_results.columns if c.startswith(prefix)]
    user_df = query_results[user_cols]
    print(user_df)

    # print(query_results)
    # print(query_results.columns)

return query_results


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
    # print("Search Tickets with")
    # for field in self._search_fields['organizations']:
    #    print(field)



