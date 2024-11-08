﻿Select
    sci.KBATCHID,
    sci.KBUSINESSENTITYID,
    sci.MATRICYEAR,
    sci.EXAMNUMBER,
    sci.EDUCATIONDEPTAVGSYMBOL,
    sci.EDUCATIONDEPTAVGPERC,
    sci.MATRICTYPE,
    sci.MATRICCERTIFICATETYPEDATE,
    sci.PRELIMREPORTYEAR,
    sci.EXAMYEARMONTH,
    sci.SCHOOLSUBJECTCODE,
    sci.NAME,
    sci.STANDARD,
    sci.MARKACHIEVED,
    sci.MARKOUTOF,
    sci.SYMBOLACHIEVED,
    sci.PERCENTAGE,
    sci.EXAMTYPECODEID,
    sci.ISSUPPLEMENTARY,
    sci.SUBJECTSELECTIONCOUNT,
    sci.EDUCATIONDEPTBUSENTID,
    sci.DEPTOFEDUCNAME,
    sci.SCHOOLBUSENTID,
    sci.SCHOOLNAME,
    sci.ADDRESS,
    sci.TOWNNAME,
    sci.ACADPROGCOUNT,
    sci.SYMBOLSTATEMENTSHOWN,
    sci.COPYOFCERTIFICATES,
    sci.MATRICCERTIFICATESHOWN,
    sci.CORLANGUAGECODEID,
    sci.FNSCCODEID,
    sci.LOCKSTAMP,
    Max(sci.AUDITDATETIME) As Max_AUDITDATETIME,
    sci.FAUDITSYSTEMFUNCTIONID,
    sci.FAUDITUSERCODE
From
    RW_SCHOOLINFO sci
Group By
    sci.KBUSINESSENTITYID,
    sci.SCHOOLSUBJECTCODE
    