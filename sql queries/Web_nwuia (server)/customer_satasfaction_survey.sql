Select
    ass.ia_assi_auto,
    ass.ia_assi_year,
    ass.ia_assi_name,
    fin.ia_find_auto,
    rem.ia_findreme_auto,
    rem.ia_findreme_employee,
    rem.ia_findreme_name,
    rem.ia_findreme_mail,
    rem.ia_findreme_auditor,
    rem.ia_findreme_auditor_email
From
    ia_assignment ass Inner Join
    ia_finding fin On fin.ia_assi_auto = ass.ia_assi_auto Inner Join
    ia_finding_remediation rem On rem.ia_find_auto = fin.ia_find_auto