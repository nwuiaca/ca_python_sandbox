Select
    find.ia_find_auto,
    cast(rate.ia_findrate_impact as integer)As ia_findrate_impact,
    cast(hood.ia_findlike_value as integer) As ia_findlike_value,
    cast(cont.ia_findcont_value as decimal(6,2)) As ia_findcont_value,
    case
      when cast(cont.ia_findcont_value as decimal(6,2)) > 0.90 then cast(rate.ia_findrate_impact as integer) * cast(hood.ia_findlike_value as integer) * 0.10
      else cast(rate.ia_findrate_impact as integer) * cast(hood.ia_findlike_value as integer) * (1 - cast(cont.ia_findcont_value as decimal(6,2)))
    end as result
From
    ia_finding find Inner Join
    ia_finding_rate rate On rate.ia_findrate_auto = find.ia_findrate_auto Inner Join
    ia_finding_likelihood hood On hood.ia_findlike_auto = find.ia_findlike_auto Inner Join
    ia_finding_control cont On cont.ia_findcont_auto = find.ia_findcont_auto