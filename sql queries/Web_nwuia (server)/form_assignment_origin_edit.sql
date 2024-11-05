Select
    orig.ia_assiorig_auto,
    orig.ia_assiorig_customer,
    orig.ia_assiorig_name,
    orig.ia_assiorig_desc,
    orig.ia_assiorig_from,
    orig.ia_assiorig_to,
    orig.ia_assiorig_active,
    orig.ia_assiorig_form
From
    ia_assignment_origin orig
Where
    -- orig.ia_assiorig_auto = '". $id."'
    orig.ia_assiorig_auto = 1