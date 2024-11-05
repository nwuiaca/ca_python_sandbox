
select * from SyAudit_Users


select * from SyDictionary order by TableName

select DISTINCT TableName from SyDictionary where TableName LIKE'Ex%'

select * from SyDictionary where TableName = 'AdProgramVersion' and ColumnName LIKE '%ID'

select * from SyDictionary where TableName = 'AdProgramVersion' and ColumnName LIKE '%ID'

select * from SyDictionary where TableName in ('AdDegree',
'AdGradeLevelPromotion',
'AdGradeScale',
'AdHonorsTable',
'AdProgramGroup',
'AdProgram',
'AdProgramVersion',
'AdSapTable',
'FaBudget',
'SaBillingMethod',
'SaCourseRefundPolicy',
'SaEarningsMethod',
'ServicerProgram',
'SyCampusGrp',
'User'
)and ColumnName LIKE '%ID'


select distinct ColumnName from SyDictionary where TableName in ('AdProgramVersion','AdCourseLevel',
'AdDegree',
'AdGradeLevelPromotion',
'AdGradeScale',
'AdHonors',
'AdHonorsTable',
'AdProgramGroup',
'AdProgram',
'AdProgramVersion',
'AdSapStatus',
'AdSapTable',
'Approver',
'CurrentAdCIPCodeYear',
'FaBudget',
'saBillingMethod',
'SaCourseRefundPolicy',
'SaEarningsMethod',
'ServicerProgram',
'SyCampusGrp',
'SyGroups',
'SySchoolStatus',
'User')and ColumnName LIKE '%ID'



select distinct TableName from SyDictionary where ColumnName in (
'SyStudentID') order by TableName



select distinct TableName from SyDictionary where ColumnName like '%mark%'


select * from SyAudit_Users


-- Program Version

SELECT        adpv.Code, adpv.Descrip AS description, adpv.CreditsReq, adpv.WeightedGPA, adpv.HoursReq, adpv.RefundBasedOn, adpv.TotalWeeks, adpv.TotalMonths, adpv.WeeksTerm, adpv.MonthsTerm, 
                         adProgram.descrip AS ProgName, syCampusGrp.descrip AS CampusGroup, adGradeScale.descrip AS GradeScale, adSapTable.descrip AS SAPTable, adDegree.descrip AS Degree, adProgramGroup.descrip AS ProgGroup
FROM            dbo.AdProgramVersion AS adpv WITH (nolock) LEFT OUTER JOIN
                         dbo.AdProgram WITH (nolock) ON adProgram.adProgramID = adpv.AdProgramID LEFT OUTER JOIN
                         dbo.SyCampusGrp WITH (nolock) ON syCampusGrp.syCampusGrpID = adpv.SyCampusGrpID LEFT OUTER JOIN
                         dbo.AdGradeScale WITH (nolock) ON adGradeScale.adGradeScaleID = adpv.AdGradeScaleID LEFT OUTER JOIN
                         dbo.AdSapTable WITH (nolock) ON adSapTable.adSapTableID = adpv.AdSapTableID LEFT OUTER JOIN
                         dbo.AdDegree WITH (nolock) ON adDegree.adDegreeID = adpv.AdDegreeID LEFT OUTER JOIN
                         dbo.AdProgramGroup WITH (nolock) ON adProgramGroup.adProgramGroupID = adpv.AdProgramGroupID



Select * from AdProgramVersion

Select * from SyCampusGrp WHERE tYPE ='p' 

Select * from SyCampusGrp  where SyCampusGrpID =35493

Select * from SyCampusGrp WHERE code = 'P~216'       

Select * from SyCampusGrp order by tYPE