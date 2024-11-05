Select
    X000aa_element_list_curr.REPORTING_NAME,
    X000aa_element_list_curr.ELEMENT_NAME,
    Count(X000aa_element_list_curr.ASSIGNMENT_ID) As Count_ASSIGNMENT_ID
From
    X000aa_element_list_curr
Group By
    X000aa_element_list_curr.REPORTING_NAME,
    X000aa_element_list_curr.ELEMENT_NAME