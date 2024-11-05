Select
    iaac.ia_assicate_name As category,
    iaat.ia_assitype_name As type,
    iaas1.ia_assistat_name As status,
    Count(assi.ia_assi_token) As assignments
From
    ia_assignment assi Left Join
    ia_assignment_category iaac On iaac.ia_assicate_auto = assi.ia_assicate_auto Left Join
    ia_assignment_type iaat On iaat.ia_assitype_auto = assi.ia_assitype_auto Left Join
    ia_assignment_status iaas1 On iaas1.ia_assistat_auto = assi.ia_assistat_auto
Where
    (assi.ia_user_sysid = 855 And
        assi.ia_assi_year > Year(Now())) Or
    (assi.ia_user_sysid = 855 And
        assi.ia_assi_year = Year(Now()) And
        assi.ia_assi_priority < 9) Or
    (assi.ia_user_sysid = 855 And
        Date(assi.ia_assi_finishdate) >= Date_Sub(Concat(Year(Now()), '-09-30'), Interval -1 Day) And
        Date(assi.ia_assi_finishdate) <= Date_Sub(Concat(Year(Now()), '-09-30'), Interval -1 Year))
Group By
    iaac.ia_assicate_name,
    iaat.ia_assitype_name,
    iaas1.ia_assistat_name