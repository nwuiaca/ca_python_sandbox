Select
    ass.ia_assi_auto As File,
    ass.ia_assi_name As Assignment,
    ori.ia_assiorig_name As Origin,
    con.ia_assicond_name As Conducted,
    cat.ia_assicate_name As Category,
    typ.ia_assitype_name As Type,
    sta.ia_assistat_name As Status
From
    ia_assignment ass Inner Join
    ia_assignment_category cat On cat.ia_assicate_auto = ass.ia_assicate_auto Inner Join
    ia_assignment_type typ On typ.ia_assitype_auto = ass.ia_assitype_auto Inner Join
    ia_assignment_status sta On sta.ia_assistat_auto = ass.ia_assistat_auto Inner Join
    ia_assignment_origin ori On ori.ia_assiorig_auto = ass.ia_assiorig_auto Inner Join
    ia_assignment_conducted con On con.ia_assicond_auto = ass.ia_assicond_auto
Where
    cat.ia_assicate_name Like 'Spec%' And
    sta.ia_assistat_name Not Like 'Com%'