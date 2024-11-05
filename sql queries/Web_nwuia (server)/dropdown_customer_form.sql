Select
    cont.id As value,
    cont.name As label
From
    jm4_contact_details cont Inner Join
    jm4_categories cate On cate.id = cont.catid
Where
    cate.extension = 'com_contact' And
    cate.title = 'Customer' And
    cont.id = 1
Order By
    label