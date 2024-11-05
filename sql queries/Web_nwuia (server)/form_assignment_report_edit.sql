Select
    repo.ia_assirepo_auto,
    repo.ia_assirepo_customer,
    repo.ia_assirepo_name,
    repo.ia_assirepo_desc,
    repo.ia_assirepo_from,
    repo.ia_assirepo_to,
    repo.ia_assirepo_active,
    repo.ia_assirepo_form
From
    ia_assignment_report repo
Where
    -- repo.ia_assirepo_auto = '". $id."'
    repo.ia_assirepo_auto = 1