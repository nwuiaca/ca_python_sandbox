

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


--Display different status of a student (approved, rejected, enrolled etc)
select * from systatchange