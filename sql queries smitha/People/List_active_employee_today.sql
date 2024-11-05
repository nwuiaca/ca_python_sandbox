Select
    pap.EMPLOYEE_NUMBER As employee_number,
    pap.PERSON_ID As person_id,
    paa.ASSIGNMENT_ID As assignment_id,
    pap.FULL_NAME As name_full,
    Upper(hrl.LOCATION_CODE) As location,
    Max(pps.ACTUAL_TERMINATION_DATE) As service_end_date,
    Upper(ppt.USER_PERSON_TYPE) As user_person_type,
    Upper(cat.MEANING) As assignment_category,
    Case
        When paa.POSITION_ID = 0
        Then Upper(paa.EMPLOYEE_CATEGORY)
        Else Upper(hrp.ACAD_SUPP)
    End As employee_category,
    Upper(hrp.POSITION_NAME) position_name,
    Upper(nat.meaning) nationality,
    Upper(pan.meaning) nationality_passport,      
    pap.PER_INFORMATION9 As is_foreign,
    pap.NATIONAL_IDENTIFIER As national_identifier,
    Upper(pap.PER_INFORMATION2) passport,
    Upper(pap.PER_INFORMATION3) permit,
    Replace(Substr(pap.PER_INFORMATION8,1,10),'/','-') permit_expire,
    acc.ORG_PAYMENT_METHOD_NAME As account_pay_method    
From
    PER_ALL_PEOPLE_F pap Left Join
    PER_ALL_ASSIGNMENTS_F paa On paa.PERSON_ID = pap.PERSON_ID
            And Date() Between paa.EFFECTIVE_START_DATE And paa.EFFECTIVE_END_DATE
            And paa.EFFECTIVE_END_DATE Between pap.EFFECTIVE_START_DATE And pap.EFFECTIVE_END_DATE
            And paa.ASSIGNMENT_STATUS_TYPE_ID = 1 Left Join
    PER_PERIODS_OF_SERVICE pps On pps.PERSON_ID = pap.PERSON_ID Left Join
    PER_PERSON_TYPE_USAGES_F ptu On pap.PERSON_ID = ptu.PERSON_ID
            And paa.EFFECTIVE_END_DATE Between ptu.EFFECTIVE_START_DATE And ptu.EFFECTIVE_END_DATE Left Join
    PER_PERSON_TYPES ppt On ptu.PERSON_TYPE_ID = ppt.PERSON_TYPE_ID Left Join
    HR_LOCATIONS_ALL hrl On hrl.LOCATION_ID = paa.LOCATION_ID Left Join
    HR_LOOKUPS cat On cat.LOOKUP_CODE = paa.EMPLOYMENT_CATEGORY
            And cat.LOOKUP_TYPE = 'EMP_CAT' Left Join
    X000_POSITIONS hrp On hrp.POSITION_ID = paa.POSITION_ID
            And paa.EFFECTIVE_END_DATE Between hrp.EFFECTIVE_START_DATE And hrp.EFFECTIVE_END_DATE Left join
    HR_LOOKUPS nat on nat.lookup_type = 'NATIONALITY' and nat.lookup_code = pap.nationality Left join
    HR_LOOKUPS pan on pan.lookup_type = 'NATIONALITY' and pan.lookup_code = pap.per_information10 Left join
    X000_pay_accounts_latest acc on acc.assignment_id = paa.assignment_id
Where
    (paa.EFFECTIVE_END_DATE Between pps.DATE_START And pps.ACTUAL_TERMINATION_DATE And
        ppt.USER_PERSON_TYPE != 'Retiree') Or
    (ppt.USER_PERSON_TYPE != 'Retiree' And
        pps.ACTUAL_TERMINATION_DATE == pps.DATE_START)
Group By
    pap.EMPLOYEE_NUMBER