-- Display table and column names for a specific table
select * from sydictionary where tablename = 'adenroll'
order by tablename, colseq

-- Display tables which contain a certain name
select * from sydictionary where tablename Like('%enrol%')
order by tablename, colseq

-- The transaction
select
bilm.saBillingMethodID, bilm.Code as BillingMethodCode, bilm.Descrip as BillingMethod,
bilm.AdCourseLevelID, coul.Code as CourseLevelCode, coul.Descrip as CourseLevel,
bild.saBillingMethodDetailID, bild.LowRange, bild.HighRange, bild.CostPerUnit, bild.UnitType, bild.FlatRate,
bild.saBillCodeID, bilc.Code as BillCode, bilc.Descrip,
codl.Code as DetailCourseLevelCode, codl.Descrip as DetailCourseLevel,
bild.SyCurrencyCodeId, curc.CurrencyCode, curc.CurrencySymbol, curc.Descrip
from
saBillingMethod as bilm
left join AdCourseLevel as coul on bilm.AdCourseLevelID = coul.AdCourseLevelID
right join saBillingMethodDetail as bild on bilm.saBillingMethodID = bild.saBillingMethodID
left join SaBillCode as bilc on bild.saBillCodeID = bilc.SaBillCodeID
left join AdCourseLevel as codl on bild.AdCourseLevelID = codl.AdCourseLevelID
left join SyCurrencyCode as curc on bild.SyCurrencyCodeId = curc.SyCurrencyCodeID
--where
--bilm.saBillingMethodID = 208
order by
BillingMethodCode

-- The databases
select SaTransid,* from SaTrans
select saAutoChargeid,* from saAutoCharge
select saBankid,* from saBank
select SaBankAccountid,* from SaBankAccount
select saBillingMethodid,* from saBillingMethod
select saBillingMethodDetailid,* from saBillingMethodDetail
select SaBillCodeid,* from SaBillCode
select SaCourseRefundPolicyid,* from SaCourseRefundPolicy
select SaPrimaryCurrencyConfigid,* from SaPrimaryCurrencyConfig
select SyCurrencyCodeID,* from SyCurrencyCode
select * from SaC
select SaTuitionDiscountPolicyid,* from SaTuitionDiscountOrder