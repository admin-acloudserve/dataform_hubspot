function email_events_joined(base_model) {



    if (constants.hubspot_contact_merge_audit_enabled == false) {
        contact =  "${ref('int_hubspot__contact_merge_adjust')}"
    }
    else {
        contact =  "${constants.contact}"
    }



    query = 
    `
    with base as (

        select *
        from ${ base_model }
    
    ), events as (
    
        select *
        from ${constants.email_event}
    
    ),
    contacts as (

        select *
        from ${contact}
    
    ), 
    events_joined as (

        select 
            base.*,
            events.created_timestamp,
            events.email_campaign_id,
            events.recipient_email_address,
            events.sent_timestamp as email_send_timestamp,
            events.sent_by_event_id as email_send_id
        from base
        left join events
            using (event_id)
    
    ),
    contacts_joined as (

        select 
            events_joined.*,
            contacts.contact_id,
            coalesce(contacts.is_contact_deleted, false) as is_contact_deleted
        from events_joined
        left join contacts
            on events_joined.recipient_email_address = contacts.email
    
    )
    select *
    from contacts_joined
    `

    return query



}



function engagements_aggregated(from_ref, primary_key) {



    query  = 
`
${primary_key},
select
${constants.primary_key},
count(case when engagement_type = 'NOTE' then ${constants.primary_key} end) as count_engagement_notes,
count(case when engagement_type = 'TASK' then ${constants.primary_key} end) as count_engagement_tasks,
count(case when engagement_type = 'CALL' then ${constants.primary_key} end) as count_engagement_calls,
count(case when engagement_type = 'MEETING' then ${constants.primary_key} end) as count_engagement_meetings,
count(case when engagement_type = 'EMAIL' then ${constants.primary_key} end) as count_engagement_emails,
count(case when engagement_type = 'INCOMING_EMAIL' then ${constants.primary_key} end) as count_engagement_incoming_emails,
count(case when engagement_type = 'FORWARDED_EMAIL' then ${constants.primary_key} end) as count_engagement_forwarded_emails
from {{ from_ref }}
group by 1
`

    return query
}


function engagement_metrics() {
    metircs = [
    'count_engagement_notes',
    'count_engagement_tasks',
    'count_engagement_calls',
    'count_engagement_meetings',
    'count_engagement_emails',
    'count_engagement_incoming_emails',
    'count_engagement_forwarded_emails'   
    ]
}


function engagements_joined (base_model) {

    let contact_ids = ''
    let deal_ids = ''
    let company_ids = ''

    if (fivetran_utils.enabled_vars([constants.hubspot_engagement_contact_enabled])) {
        contact_ids  = "engagements.contact_ids"
    }
    
    if (fivetran_utils.enabled_vars([constants.hubspot_engagement_deal_enabled])) {
        deal_ids  = "engagements.deal_ids"
    }

    if (fivetran_utils.enabled_vars([constants.hubspot_engagement_company_enabled])) {
        company_ids  = "engagements.company_ids"
    }

    query  = 
`
with base as (

    select *
    from ${ base_model }

), engagements as (

    select *
    from ${ ref('hubspot__engagements') }

),
joined as (

    select 
        base.*,
        ${contact_ids},
        ${deal_ids},
        ${company_ids},
        engagements.is_active,
        engagements.created_timestamp,
        engagements.occurred_timestamp,
        engagements.owner_id
    from base
    left join engagements
        using (engagement_id)

)

select *
from joined

`

}


module.exports = { 
    email_events_joined, 
    engagements_aggregated,
    engagement_metrics,
    engagements_joined

};