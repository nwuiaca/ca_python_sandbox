Select
    dtab.ia_assistat_auto As value,
    Case
        When dtab.ia_assistat_active = 1
        Then Concat(cont.name, ' - ', cate.ia_assicate_name, ' - ', dtab.ia_assistat_name, ' (Active) ',
            Date(dtab.ia_assistat_from), '/', Date(dtab.ia_assistat_to))
        Else Concat(cont.name, ' - ', cate.ia_assicate_name, ' - ', dtab.ia_assistat_name, ' (InActve) ',
            Date(dtab.ia_assistat_from), '/', Date(dtab.ia_assistat_to))
    End As label
From
    ia_assignment_status dtab Inner Join
    jm4_contact_details cont On cont.id = dtab.ia_assistat_customer Inner Join
    ia_assignment_category cate On cate.ia_assicate_auto = dtab.ia_assicate_auto
Where
    -- dtab.ia_assicate_auto = ".$record_category."
    dtab.ia_assicate_auto = 11
Order By
    label