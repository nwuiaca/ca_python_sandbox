
-- Course (Module)


-- Display table and column names for a specific table
select * from sydictionary (nolock) where tablename = 'AdCourse'
order by tablename, colseq


-- Display all Course (Module)
select * from AdCourse (nolock)


-- Display all SyCampusGrp
select * from SyCampusGrp (nolock) -- Type should be "U" for user created group

-- Display all SyCampusList
select * from SyCampusList (nolock)

-- Display all campus
select * from SyCampus (nolock)


-- Display all Course type (Lecturers, Practical etc)
select * from AdCourseType (nolock)

-- Display year level of the Course offered (Module level)
select * from AdCourseLevel (nolock)

-- Display the Course Prerequisites that are configuired (Module Prerequisites configuired)
select * from AdCoursePreReqConfig (nolock)

-- Display the Course Prerequisites  (Module Prerequisites)
select * from AdCoursePreReq (nolock) -- Table is empty

-- Display the Course Prerequisites rule "AND or OR rule "that are configuired (Module Prerequisites configuired)
select * from AdCoursePreReqRule (nolock)

select AdCourse.AdCourseID,
AdCourse.Code,
AdCourse.Descrip,
SyCampusGrp.Code AS CampusGroupCode,
SyCampusGrp.Descrip AS CampusGroupDescrip,
SyCampusList.SyCampusID,
SyCampus.SyCampusID,
SyCampus.Code AS CampusCode,
SyCampus.Descrip,
AdCourse.AdCourseTypeID,
AdCourseType.Code AS CourseTypeCode,
AdCourseType.Descrip AS CourseTypeDescrip,
AdCourse.AdCourseLevelID,
AdCourseLevel.Code,
AdCourseLevel.Descrip,
* from AdCourse (nolock)
LEFT JOIN SyCampusGrp (nolock) 
	on AdCourse.SyCampusGrpID = SyCampusGrp.SyCampusGrpID
LEFT JOIN SyCampusList (nolock) 
	on AdCourse.SyCampusGrpID = SyCampusList.SyCampusGrpID
LEFT JOIN SyCampus (nolock) 
	on SyCampusList.SyCampusID = SyCampus.SyCampusID
LEFT JOIN AdCourseType (nolock) 
	on AdCourse.AdCourseTypeID = AdCourseType.AdCourseTypeID
LEFT JOIN AdCourseLevel (nolock) 
	on AdCourse.AdCourseLevelID = AdCourseLevel.AdCourseLevelID


-- AdDependentCourseID  No such table could find.
-- AdWlmDepartmentId empty table
