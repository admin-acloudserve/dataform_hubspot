-- config {
--     type: "table"
--   }

-- js {
  
--     function run() {
--       if (fivetran_utils.enabled_vars([constants.hubspot_marketing_enabled, constants.hubspot_contact_enabled, constants.hubspot_contact_merge_audit_enabled])) {
          
--           query = 
-- `
-- with contacts as (

--     select *
--     from ${constants.contact}

-- ), 
-- contact_merge_audit as (
--     select *
--     from ${constants.contact_merge_audit}

-- ), 
-- contact_merge_removal as (
--     select 
--         contacts.*
--     from contacts
    
--     left join contact_merge_audit
--         on contacts.contact_id = contact_merge_audit.vid_to_merge
    
--     where contact_merge_audit.vid_to_merge is null
-- )
-- select *
-- from contact_merge_removal


-- `
--         return query
--       }
--       return
--     }
-- }
  
--   ${run()}

