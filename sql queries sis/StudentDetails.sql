

-- Student details


select distinct TableName from sydictionary (nolock) where tablename like '%saudit%'
order by tablename, colseq

-- Display student details systudentId is the primary key
select* from systudent (nolock)


-- Display all Sex codes
select * from amSex (nolock)

-- Display all enrollment details adenrollId is the primary key and systudentId is the foreign key
select * from adenroll (nolock)

-- Display all application details
select * from SmApplication (nolock)


select * from SmApplicationChoice (nolock)


-- Display all shift details (Contact, Distance etc)

select * from AdShift


-- Display  selection status details

select * from SmSelectionStatus


-- Display  all the meaning of each fields

select * from SyExtendedPropertyDefinition where entityName like 'Applicant'


-- Display  all the meaning of each fields code values
select * from SyExtendedPropertyValue 

-- Display  all the tasks of each student. One student may have may records.All events including emails
select * from cmevent

-- Display  all the documents submitted or required by the student.There is a ModuleId connected to each document.
select * from cmdocument

--Display address of the student . SyAddrTypeID gives the address type
select * from SyAddress where SyStudentID = 3104

--Display the address type of each SyAddrTypeID

select * from SyAddrType where SyAddrTypeID =14

--Display the systatus  of each systatusID
select * from systatus

--Display the systatus  of each systatusID
select * from SySchoolStatus


--Display different status of a student (approved, rejected, enrolled etc)
select * from systatchange


--Display different status and these status will have a step by step change and not related to student
select * from SySchoolStatus (nolock)

--Display  status change of a student , what was the previous status and what is the new status etc Module Link Ststus history
select * from systatchange (nolock)

select AmTitle.AmTitleID,
AmTitle.Code AS AmTitleCode,
AmTitle.Descrip,
SyCountry.SyCountryID,
SyCountry.Code,
SyCountry.Descrip,
amSex.amSexID,
amSex.Code,
amSex.Descrip,
AmRace.AmRaceID,
AmRace.Code,
AmRace.Descrip,
AmNationality.AmNationalityID,
AmNationality.Code,
AmNationality.Descrip,
AmMarital.AmMaritalID,
AmMarital.Code,
AmMarital.Descrip,
AmCitizen.AmCitizenID,
AmCitizen.Code,
AmCitizen.Descrip,
SyCampus.SyCampusID,
SyCampus.Code,
SyCampus.Descrip,
AdProgramGroup.AdProgramGroupID,
AdProgramGroup.Code,
AdProgramGroup.Descrip,
AdProgram.AdProgramID,
AdProgram.Code,
AdProgram.Descrip,

SySchoolStatus.SySchoolStatusID,
SySchoolStatus.Code,
SySchoolStatus.Descrip,

AmRep.AmRepID,
AmRep.Code,
AmRep.Descrip,
AmRep.RepTypeCode,
AmRep.RepTypeDescrip,
SyPerson.SyPersonId,
SyPerson.FullName,
* from systudent (nolock)
LEFT JOIN AmTitle
	on AmTitle.AmTitleID = systudent.AmTitleID
LEFT JOIN SyCountry
	on SyCountry.SyCountryID = systudent.SyCountryID
LEFT JOIN amSex
	on amSex.amSexID = systudent.amSexID
LEFT JOIN AmRace
	on AmRace.AmRaceID = systudent.AmRaceID
LEFT JOIN AmNationality
	on AmNationality.AmNationalityID = systudent.AmNationalityID
LEFT JOIN AmMarital
	on AmMarital.AmMaritalID = systudent.AmMaritalID
LEFT JOIN AmCitizen
	on AmCitizen.AmCitizenID = systudent.AmCitizenID
LEFT JOIN SyCampus
	on SyCampus.SyCampusID = systudent.SyCampusID
LEFT JOIN AdProgramGroup
	on AdProgramGroup.AdProgramGroupID = systudent.AdProgramGroupID
LEFT JOIN AdProgram
	on AdProgram.AdProgramID = systudent.AdProgramID
LEFT JOIN AmRep
	on AmRep.AmRepID = systudent.AmRepID
LEFT JOIN SyPerson
	on SyPerson.SyPersonID = systudent.SyPersonID
LEFT JOIN SySchoolStatus
	on SySchoolStatus.SySchoolStatusID = systudent.SySchoolStatusID	
	