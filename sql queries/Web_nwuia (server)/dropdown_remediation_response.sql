Select
    resp.ia_findresp_auto As value,
    resp.ia_findresp_name As label
From
    ia_finding_response resp
Where
    resp.ia_findresp_active = 1 And
    -- resp.ia_findresp_customer = ".$customer_id." And
    resp.ia_findresp_customer = 1 And
    -- resp.ia_findresp_from <= '".$create_date."' And
    resp.ia_findresp_from <= Now() And
    -- resp.ia_findresp_from >= '".$create_date."'
    resp.ia_findresp_to >= Now()
Order By
    label