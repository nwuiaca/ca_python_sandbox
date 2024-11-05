Select
    peop.employee_number,
    peop.name_address,
    peop.assignment_id,
    peop.assignment_number,
    peop.assignment_category,
    peop.employee_category,
    peop.user_person_type,
    X004_CONTRACT_CURR.CONTRACT_NUMBER,
    X004_CONTRACT_CURR.PERSON_TYPE,
    X004_CONTRACT_CURR.CONTRACT_CATEGORY,
    X004_CONTRACT_CURR.CONTRACT_TYPE,
    X004_CONTRACT_CURR.CONTRACT_FROM,
    X004_CONTRACT_CURR.CONTRACT_TO
From
    X000_PEOPLE peop Left Join
    X004_CONTRACT_CURR On X004_CONTRACT_CURR.ASSIGNMENT_ID = peop.assignment_id
      And X004_CONTRACT_CURR.CONTRACT_FROM <= Date('now')
      And X004_CONTRACT_CURR.CONTRACT_TO >= Date('now')
