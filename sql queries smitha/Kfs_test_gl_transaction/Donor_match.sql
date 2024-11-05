Select
    don.*,
    X003b_donor_matches.Donor_name
From
    X003a_donor_transaction don Left Join
    X003b_donor_matches On X003b_donor_matches.Unique_field = don.Unique_field