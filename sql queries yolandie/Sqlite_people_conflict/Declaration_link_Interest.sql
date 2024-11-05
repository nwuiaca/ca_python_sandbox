Select
    X001_declarations_curr.DECLARATION_ID,
    X001_declarations_curr.EMPLOYEE,
    X001_declarations_curr.EMP_NAME,
    X001_declarations_curr.EMP_SURNAME,
    X001_declarations_curr.DECLARATION_DATE,
    X001_declarations_curr.FULL_DISCLOSURE_FLAG,
    X001_declarations_curr.INTEREST_TO_DECLARE_FLAG,
    X001_declarations_curr.UNDERSTAND_POLICY_FLAG,
    X001_declarations_curr.STATUS,
    X001_declarations_curr.REJECTION_REASON,
    X002_interests_curr.INTEREST_ID,
    X002_interests_curr.CONFLICT_TYPE,
    X002_interests_curr.INDUSTRY_TYPE,
    X002_interests_curr.INTEREST_STATUS,
    X002_interests_curr.ENTITY_NAME,
    X002_interests_curr.ENTITY_REGISTRATION_NUMBER,
    X002_interests_curr.DESCRIPTION,
    X002_interests_curr.DIR_APPOINTMENT_DATE,
    X002_interests_curr.INTEREST_TYPE,
    X002_interests_curr.PERC_SHARE_INTEREST,
    X002_interests_curr.MITIGATION_AGREEMENT,
    X002_interests_curr.REJECTION_REASON As REJECTION_REASON1,
    X002_interests_curr.TASK_PERF_AGREEMENT,
    X002_interests_curr.LINE_MANAGER
From
    X001_declarations_curr Left Join
    X002_interests_curr On X002_interests_curr.DECLARATION_ID = X001_declarations_curr.DECLARATION_ID