Select
    decl.DECLARATION_ID,
    decl.EMPLOYEE,
    decl.EMP_SURNAME,
    intr.INTEREST_ID,
    intr.EMP_SURNAME As EMP_SURNAME1,
    intr.ENTITY_NAME
From
    X001_declarations_prev decl Left Join
    X002_interests_prev intr On intr.DECLARATION_ID = decl.DECLARATION_ID