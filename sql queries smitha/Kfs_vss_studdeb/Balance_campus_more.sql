Select
    X002ab_vss_transort.STUDENT_VSS,
    X002ab_vss_transort.CAMPUS_VSS,
    Total(X002ab_vss_transort.AMOUNT_VSS) As Total_AMOUNT_VSS,
    Count(X002ab_vss_transort.BURSCODE_VSS) As Count_BURSCODE_VSS
From
    X002ab_vss_transort
Group By
    X002ab_vss_transort.STUDENT_VSS,
    X002ab_vss_transort.CAMPUS_VSS