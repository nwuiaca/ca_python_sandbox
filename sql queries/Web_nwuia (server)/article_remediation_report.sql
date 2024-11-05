Select
  assi.ia_assi_auto,
  assi.ia_assi_name,
  assi.ia_assi_report,
  cond.ia_assicond_name,
  reme.ia_findreme_auditor,
  assi.ia_assi_year,
  assi.ia_assi_file,
  assi.ia_assi_period,
  type.ia_assitype_name,
  site.ia_assisite_name,
  repo.ia_assirepo_name,
  find.ia_find_name,
  find.ia_find_riskmatrix_toggle,
  rate.ia_findrate_impact,
  rate.ia_findrate_name,
  rate.ia_findrate_desc,
  lhoo.ia_findlike_value,
  lhoo.ia_findlike_name,
  lhoo.ia_findlike_desc,
  cont.ia_findcont_value,
  cont.ia_findcont_name,
  cont.ia_findcont_desc,
  clra.ia_findrate_impact As ia_findrate_impact_client,
  clra.ia_findrate_name As ia_findrate_name_client,
  clra.ia_findrate_desc As ia_findrate_desc_client,
  clli.ia_findlike_value As ia_findlike_value_client,
  clli.ia_findlike_name As ia_findlike_name_client,
  clli.ia_findlike_desc As ia_findlike_desc_client,
  clco.ia_findcont_value As ia_findcont_value_client,
  clco.ia_findcont_name As ia_findcont_name_client,
  clco.ia_findcont_desc As ia_findcont_desc_client,
  find.ia_find_desc,
  find.ia_find_risk,
  find.ia_find_criteria,
  find.ia_find_procedure,
  find.ia_find_condition,
  find.ia_find_effect,
  find.ia_find_cause,
  find.ia_find_recommend,
  find.ia_find_comment,
  find.ia_find_frequency,
  find.ia_find_definition,
  find.ia_find_reference,
  find.ia_find_var01,
  find.ia_find_var02,
  find.ia_find_var03,
  find.ia_find_var04,
  find.ia_find_var05,
  find.ia_find_var06,
  find.ia_find_var07,
  find.ia_find_var08,
  find.ia_find_var09,
  find.ia_find_var10,
  find.ia_find_var11,
  find.ia_find_var12,
  assi.ia_assi_header,
  assi.ia_assi_header_text,
  assi.ia_assi_report_text1,
  assi.ia_assi_report_text2,
  assi.ia_assi_footer,
  assi.ia_assi_signature
From
  ia_finding_remediation reme Inner Join
  ia_finding find On find.ia_find_auto = reme.ia_find_auto Inner Join
  ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
  ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto Left Join
  ia_assignment_site site On site.ia_assisite_auto = assi.ia_assisite_auto Left Join
  ia_assignment_report repo On repo.ia_assirepo_auto = assi.ia_assirepo_auto Left Join
  ia_finding_rate rate On rate.ia_findrate_auto = find.ia_findrate_auto Left Join
  ia_finding_rate clra On clra.ia_findrate_auto = find.ia_findrate_auto_client Left Join
  ia_finding_likelihood lhoo On lhoo.ia_findlike_auto = find.ia_findlike_auto Left Join
  ia_finding_likelihood clli On clli.ia_findlike_auto = find.ia_findlike_auto_client Left Join
  ia_finding_control cont On cont.ia_findcont_auto = find.ia_findcont_auto Left Join
  ia_finding_control clco On clco.ia_findcont_auto = find.ia_findcont_auto_client Left Join
  ia_assignment_conducted cond On cond.ia_assicond_auto = assi.ia_assicond_auto
Where
  reme.ia_findreme_mail_trigger > 0 And
  -- reme.ia_findreme_token = '".$hash."'
  reme.ia_findreme_token = '759e3eaf6d076dc1937b43e7f3c0cd07'  