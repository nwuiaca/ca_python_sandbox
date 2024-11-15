

-- 	Class Schedule details


select distinct TableName from sydictionary (nolock) where tablename like '%saudit%'
order by tablename, colseq

-- Display Adclasssched details

select* from Adclasssched(nolock)

-- Display AdTeacher details (only Primary Instructor)

select * from AdTeacher (nolock)

-- Display only Secondary Instructor

select * from AdclassSchedInstructor (nolock)

--Class Schedules can be Daily, Weekly and Monthly

-- Display all the courses ( modules )linked to a specific AdProgramVersion

select* from AdProgramCourse (nolock) order by AdProgramVersionID


