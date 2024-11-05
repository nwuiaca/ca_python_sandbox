Select
    assi.ia_assi_auto,
    assi.ia_assi_token,
    Concat(assi.ia_assi_name, ' (', assi.ia_assi_auto, ')') As ia_assi_name,
    assi.ia_assi_priority,
    assi.ia_assistat_auto,
    assi.ia_assicate_auto,
    assi.ia_assi_desc,
    assi.ia_assi_offi,
    assi.ia_assi_report_toggle,
    assi.ia_assi_report,
    assi.ia_assi_proofdate,
    assi.ia_assi_finishdate
From
    ia_assignment assi
Where
    assi.ia_assi_auto = 397 And
    assi.ia_assi_token = 'f3d0f05951cf5dd93c5eacff102a251e'