with

source as (

    select * from {{ source('ecom', 'raw_leads') }}

),

adjusted as (

    select

        ----------  ids
        lead_uuid,

        ---------- properties
        entry_date,
        DATEFROMPARTS(YEAR(entry_date), MONTH(entry_date), 1) AS entry_month,
        lead_number,
        email_hash,
        phone_hash,
        city,
        state,
        zip,
        appt_datetime,
        set,
        demo,
        dispo,
        job_status

    from source

)

select * from adjusted
