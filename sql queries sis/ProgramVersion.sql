
-- Program Version (Qualification)


-- Display table and column names for a specific table
select * from sydictionary (nolock) where tablename = 'AdProgramVersion'
order by tablename, colseq


-- Display all SyCampusGrp
select * from SyCampusGrp (nolock) -- Type should be "U" for user created group

-- Display all SyCampusList
select * from SyCampusList (nolock)

-- Display all campus
select * from SyCampus (nolock)



-- Display the list of ProgramGroup (Faculties)
select * from AdProgramGroup (nolock)


-- Display the list of Program (Schools )
select * from AdProgram (nolock)


-- Display the list of Program Versions (Qualification )
select * from AdProgramVersion (nolock)


-- Display the list of Program Version's start and end date per year's student
select * from AdStartDate (nolock)

-- Display the description of different code values of fields in all tables
select * from SyCode (nolock)

-- Display all the Grade scale (UG,Masters,Doctoral etc)
select * from AdGradeScale

-- Display the details of Program Versions (Qualification )


select AdStartDate.AdStartDateID,
AdStartDate.Code,
AdStartDate.Descrip as AdStartDateDescrip,
AdStartDate.Active as AdStartDateActiveStatus,
SyCampusGrp.Code as SyCampusGrpCode,
SyCampusGrp.Descrip as SyCampusGrpDescrip,
SyCampusList.SyCampusID,
SyCampus.SyCampusID,
SyCampus.Code as SyCampusCode,
SyCampus.Descrip as SyCampusDescrip,
AdProgramVersion.Active as AdProgramVersionActiveStatus,
AdProgramVersion.AdProgramVersionID,
AdProgramVersion.Code as AdProgramVersionCode,
AdProgramVersion.Descrip as AdProgramVersionDescrip,
AdProgramVersion.SyCampusGrpID ,
AdDegree.AdDegreeID,
AdDegree.Code as AdDegreeCode,
AdDegree.Descrip as AdDegreeDescrip,
AdProgramVersion.AdGradeScaleID,
AdGradeScale.Code AS GradeScaleCode,
AdGradeScale.Descrip AS GradeScaleDescrip,
AdGradeScale.Active as AdGradeScaleActiveStatus ,
AdProgram.AdProgramID,
AdProgram.Code as AdProgramCode,
AdProgram.Descrip as AdProgramDescrip,
AdProgramGroup.AdProgramGroupID,
AdProgramGroup.Code as AdProgramGroupCode,
AdProgramGroup.Descrip as AdProgramGroupDescrip
from AdProgramVersion (nolock)
LEFT JOIN AdProgram 
	on AdProgramVersion.AdProgramID = AdProgram.AdProgramID
LEFT JOIN AdProgramGroup 
	on AdProgramVersion.AdProgramGroupID = AdProgramGroup.AdProgramGroupID
LEFT JOIN AdDegree 
	on  AdProgramVersion.AdDegreeID = AdDegree.AdDegreeID
LEFT JOIN SyCampusGrp 
	on AdProgramVersion.SyCampusGrpID =SyCampusGrp.SyCampusGrpID
LEFT JOIN SyCampusList 
	on AdProgramVersion.SyCampusGrpID = SyCampusList.SyCampusGrpID
LEFT JOIN SyCampus 
	on SyCampusList.SyCampusID = SyCampus.SyCampusID
LEFT JOIN AdStartDate 
	on AdProgramVersion.AdProgramVersionID = AdStartDate.AdProgramVersionID
LEFT JOIN AdGradeScale 
	on AdProgramVersion.AdGradeScaleID = AdGradeScale.AdGradeScaleID
where AdStartDate.Active = 1
order by AdProgramGroup.Descrip,AdProgram.Descrip,AdDegree.Descrip,AdProgramVersion.Descrip
