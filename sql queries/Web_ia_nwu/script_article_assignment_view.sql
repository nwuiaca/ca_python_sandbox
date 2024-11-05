Select
    assi.ia_assi_auto,
    assi.ia_assi_name,
    assi.ia_assi_report,
    assi.ia_assi_header,
    assi.ia_assi_footer,
    assi.ia_assi_signature
From
    ia_assignment assi
Where
    assi.ia_assi_token = 'hash'