Select
    X002aa_log_history."ACTION",
    X002aa_log_history.OBJECT,
    Avg(X002aa_log_history.LOG_SECOND) As Avg_LOG_SECOND,
    Count(X002aa_log_history.LOG_ELAPSED) As Count_LOG_ELAPSED
From
    X002aa_log_history
Where
    X002aa_log_history."ACTION" = "WRITE TABLE"
Group By
    X002aa_log_history."ACTION",
    X002aa_log_history.OBJECT