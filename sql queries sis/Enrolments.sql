

-- Enrollments details


select distinct TableName from sydictionary (nolock) where tablename like '%saudit%'
order by tablename, colseq

-- Display Adenrollterm details

select* from Adenrollterm (nolock)

-- Display Enrollments details

select * from Adenroll (nolock)

-- Display all the courses ( modules )linked to a specific Adenrollid

select* from Adenrollsched (nolock)


-- Display all the courses ( modules )linked to a specific AdProgramVersion

select* from AdProgramCourse (nolock) order by AdProgramVersionID



-- Display all the courses that a student may register will be listed. This needs to be selected using systudentid and AdenrollID. There can be more than one AdenrollID for a student
Select * from Systudent (nolock) where systudentid = 61678
select AdenrollID,AdProgramversionID,* FROM Adenroll (nolock) where SystudentId = 61678
select AdenrollschedId, * from Adenrollsched where adenrollid = 62234
--