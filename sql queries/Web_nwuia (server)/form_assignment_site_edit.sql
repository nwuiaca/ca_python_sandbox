Select
    site.ia_assisite_auto,
    site.ia_assisite_customer,
    site.ia_assisite_name,
    site.ia_assisite_desc,
    site.ia_assisite_from,
    site.ia_assisite_to,
    site.ia_assisite_active,
    site.ia_assisite_form
From
    ia_assignment_site site
Where
    -- site.ia_assisite_auto = '". $id."'
    site.ia_assisite_auto = 1