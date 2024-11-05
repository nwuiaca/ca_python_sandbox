-- Display table and column names for a specific table
select * from sydictionary where tablename = 'adenroll'
order by tablename, colseq

-- Display tables which contain a certain name
select * from sydictionary where tablename Like('%enrol%')
order by tablename, colseq

-- List of enrollments
SELECT
enro.AdEnrollID,
enro.SyStudentID, enro.StuNum, stud.SSN AS IDno, titl.Descrip AS Title, stud.NickName AS Initial, stud.LastName, stud.FirstName,
enro.AppRecDate,
enro.EnrollDate,
enro.ExpStartDate,
enro.amRepID, repr.Descrip AS Representative,
enro.SyCampusID, camp.Code As CampusCode, camp.Descrip AS Campus,
prov.adProgramGroupid, prgr.Code AS ProgramGroupCode, prgr.Descrip AS [ProgramGroup(Faculty)],
enro.sySchoolStatusid, scho.Code AS SchoolCode, scho.Descrip AS SchoolStatus, scho.StatusDescription,
enro.adGradeLevelid, gral.Code AS GradeLevelCode, gral.Descrip AS GradeLevel,
enro.adTermid, term.Code AS TermCode, term.Descrip AS EnrollTerm,
enro.adAttStatid, atts.Code AS EnrolStatusCode, atts.Descrip As EnrolStatus,
enro.StatusDate,
enro.adShiftid, shif.Code AS ShiftCode, shif.Descrip AS [Shift(PresentCategory)],
enro.AdProgramid, prog.Code AS ProgramCode, prog.Descrip AS [Program(School)],
enro.AdProgramVersionid, prov.Code AS ProgramVersionCode, prov.Descrip AS [ProgramVersion(Qualification)], enro.adProgramDescrip AS [Program(School)User],
prov.adDegreeid, degr.Code AS DegreeCode, degr.Descrip AS [Degree(QualificationType)],
enro.adGradeScaleid, gras.Code AS GradeScaleCode, gras.Descrip AS GradeScale,
enro.adStartDateid, star.Code AS StartCode, star.Descrip As StartDescription,
enro.adReasonid, reas.Code AS ReasonCode, reas.Descrip AS ReasonDescription,
enro.DropDate,
enro.OriginalExpStartDate,
enro.OrigGradDate,
enro.GradDate,
enro.MidDate,
enro.LastActivityDate
FROM
dbo.AdEnroll AS enro
LEFT JOIN dbo.syStudent AS stud ON enro.SyStudentID = stud.SyStudentId
LEFT JOIN dbo.amTitle AS titl ON stud.AmTitleID = titl.amTitleID
LEFT JOIN dbo.amRep as repr ON enro.amRepID = repr.amRepID
LEFT JOIN dbo.syCampus AS camp ON enro.syCampusID = camp.syCampusID
LEFT JOIN dbo.sySchoolStatus AS scho ON enro.sySchoolStatusid = scho.sySchoolStatusid
LEFT JOIN dbo.adGradeLevel AS gral ON enro.adGradeLevelid = gral.adGradeLevelid
LEFT JOIN dbo.adTerm AS term ON enro.adTermid = term.adTermid
LEFT JOIN dbo.adAttStat AS atts ON enro.adAttStatid =atts.adAttStatid
LEFT JOIN dbo.adShift AS shif ON enro.adShiftid = shif.adShiftid
LEFT JOIN dbo.AdProgram As prog ON enro.AdProgramid = prog.AdProgramid
LEFT JOIN dbo.AdProgramVersion AS prov ON enro.AdProgramVersionid = prov.AdProgramVersionid
LEFT JOIN dbo.adGradeScale AS gras ON enro.adGradeScaleid = gras.adGradeScaleid
LEFT JOIN dbo.adStartDate AS star ON enro.adStartDateid = star.adStartDateid
LEFT JOIN dbo.adReason AS reas ON enro.adReasonid = reas.adReasonid
LEFT JOIN dbo.adDegree AS degr ON prov.adDegreeid = degr.adDegreeid
LEFT JOIN dbo.adProgramGroup AS prgr ON prov.adProgramGroupid = prgr.adProgramGroupid
ORDER BY enro.SyStudentID, enro.StuNum

-- Tables
select adenrollid,* from adenroll
select systudentid,* from systudent
select systudentid,* from adenroll where systudentid = 1951
select amTitleid,* from amTitle
select amRepID,* from amRep
select sycampusid,* from sycampus
select sySchoolStatusid,* from SySchoolStatus
select AdGradeLevelid,* from AdGradeLevel
select adTermid,* from adTerm
select adTermGroupid,* from adTermGroup
select adAttStatid,* from adAttStat
select adShiftid,* from adShift
select adProgramGroupid,* from adProgramGroup
select AdProgramid,* from AdProgram
select AdProgramVersionid,* from AdProgramVersion
select adGradeScaleid,* from adGradeScale
select adStartDateid,* from adStartDate
select adReasonid,* from adReason
select SyCampusGrpid,* from SyCampusGrp
select adSapStatus,* from adSapStatus
select adRegistrationCohortid,* from adRegistrationCohort
select adDegreeid,* from adDegree


-- Identify students with more than one enrolment
SELECT SyStudentID, COUNT(*) AS occurrence_count
FROM dbo.AdEnroll
GROUP BY SyStudentID
HAVING (COUNT(*) > 1)

-- Identify students with more than one enrolment
SELECT SyStudentID, COUNT(*) AS occurrence_count
FROM dbo.AdEnroll
GROUP BY SyStudentID, adenrollid
HAVING (COUNT(*) > 1)

-- Identify students with more than one school status
SELECT SyStudentID, COUNT(*) AS occurrence_count
FROM dbo.AdEnroll
GROUP BY SyStudentID, adenrollid, syschoolstatusid
HAVING (COUNT(*) > 1)

-- Identify students with more than one program version
SELECT SyStudentID, COUNT(*) AS occurrence_count
FROM dbo.AdEnroll
GROUP BY SyStudentID, adenrollid, adprogramversionid
HAVING (COUNT(*) > 1)

