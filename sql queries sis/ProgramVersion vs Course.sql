
-- Program Version (Qualification) vs Course (Module)


-- Display table and column names for a specific table
select * from sydictionary (nolock) where tablename = 'AdProgramGroup'
order by tablename, colseq


-- Display all Programs (Schools) 
select * from AdProgram (nolock)


-- Display all Programs Verasion (Qualifications).. More than one ProgramsGroup ( Faculties )can be linked to a Program Version (Qualification).

select * from AdProgramVersion (nolock)

-- More than one ProgramsGroup ( Faculties )can be linked to a Program Version (Qualification).

select * from AdProgramProgramGroup (nolock)

--Not used by NWU
select * from AdRequirementrule (nolock)

--Sap Policy

select * from AdSapTable

-- Display all the Grade scale (UG,Masters,Doctoral etc)
select * from AdGradeScale

-- Display all Course (module) id that are linked to a ProgramVersion (Qualification)
select * from AdProgramCourse (nolock)

-- Display all courses (Modules)
select * from AdCourse

-- Display all courses (Modules)Year Level
select * from AdCourseLevel

select * from AdCourseType




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
AdGradeScale.Code as GradeScaleCode,
AdGradeScale.Descrip as GradeScaleDescrip,
AdGradeScale.Active as AdGradeScaleActiveStatus ,
AdProgram.AdProgramID,
AdProgram.Code as AdProgramCode,
AdProgram.Descrip as AdProgramDescrip,
AdProgramGroup.AdProgramGroupID,
AdProgramGroup.Code as AdProgramGroupCode,
AdProgramGroup.Descrip as AdProgramGroupDescrip,
AdProgramCourse.AdCourseID,
AdCourse.Code as AdCourseCode,
AdCourse.Descrip as AdCourseDescrip,
AdProgramCourse.AdProgramElectivePoolID,
AdProgramCourse.ElectivePoolDescrip,
AdCourse.AdCourseTypeID,
AdCourseLevel.Code as AdCourseLevelCode,
AdCourseLevel.Descrip as AdCourseLevelDescrip,
AdCourseLevel.Active as AdCourseLevelActiveStatus,
AdCourseType.Code as AdCourseTypeCode,
AdCourseType.Descrip as AdCourseTypeDescrip,
AdCourseType.Active as AdCourseTypeActiveStatus,
AdCoursePreReqConfig.AdCoursePreReqConfigID,
AdCoursePreReqConfig.PreReqType,
AdCoursePreReqConfig.Enabled as AdCoursePreReqConfigEnabledStatus,
AdCoursePreReqConfig.MinValueRequired,
AdCoursePreReqConfig.IsOptional as AdCoursePreReqConfigIsOptionalStatus
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
LEFT JOIN AdProgramCourse 
	on AdProgramCourse.AdProgramVersionID = AdProgramVersion.AdProgramVersionID
LEFT JOIN AdCourse 
	on AdProgramCourse.AdCourseID = AdCourse.AdCourseID
LEFT JOIN AdProgramElectivePool 
	on AdProgramCourse.AdProgramElectivePoolID =AdProgramElectivePool.AdProgramElectivePoolID
LEFT JOIN AdCourseLevel 
	on AdCourse.AdCourseLevelID =AdCourseLevel.AdCourseLevelID
LEFT JOIN AdCourseType 
	on AdCourse.AdCourseTypeID = AdCourseType.AdCourseTypeID
LEFT JOIN AdCoursePreReqConfig 
	on AdCourse.AdCourseID = AdCoursePreReqConfig.AdCourseID
WHERE AdProgramVersion.AdProgramVersionID =218
and  AdStartDate.Active = 1
order by AdProgramGroup.Descrip,AdProgram.Descrip,AdDegree.Descrip,AdProgramVersion.Descrip


select * from AdProgramVersion (nolock) where AdProgramVersionID =218
select * from AdProgramCourse (nolock) where AdProgramVersionID =218
select * from AdCourse