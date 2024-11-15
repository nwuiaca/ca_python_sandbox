Select
    par.KBUSINESSENTITYID As student,
    par.PARTYTYPE,
    Case
      When par.PARTYTYPE = 1 Then tit.CODESHORTDESCRIPTION || ' ' || par.INITIALS || ' ' || par.SURNAME
      Else par.NAME
    End as name_address,
    par.FIRSTNAMES as first_names,
    par.NICKNAME as nickname
From
    X000_Party par Left Join
    CODEDESCRIPTION tit On tit.KCODEDESCID = par.FTITLECODEID And tit.KSYSTEMLANGUAGECODEID = 3
    