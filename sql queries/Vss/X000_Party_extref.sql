﻿SELECT
  X000_Party.KBUSINESSENTITYID,
  X005aa_Party_extref.KEXTERNALREFERENCECODEID,
  X005aa_Party_extref.LONG,
  X005aa_Party_extref.LANK,
  X005aa_Party_extref.EXTERNALREFERENCENUMBER
FROM
  X000_Party
  LEFT JOIN X005aa_Party_extref ON X005aa_Party_extref.KBUSINESSENTITYID = X000_Party.KBUSINESSENTITYID AND
    X005aa_Party_extref.STARTDATE <= Date('2018-06-25') AND X005aa_Party_extref.ENDDATE >= Date('2018-06-25')
WHERE
  X005aa_Party_extref.KEXTERNALREFERENCECODEID IS NOT NULL AND
  X005aa_Party_extref.KEXTERNALREFERENCECODEID <> '6715'