Select
    assi.ia_assi_priority,
    Count(assi.ia_assi_auto) As Count_ia_assi_auto
From
    ia_assignment assi
Group By
    assi.ia_assi_priority