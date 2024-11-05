Select
    X001ad_auto_time.LOG_DATE,
    X001ad_auto_time.LOG_TIME,
    X001ad_auto_time.SCRIPT,
    X001ad_auto_time."DATABASE",
    X001ad_auto_time."ACTION"
From
    X001ad_auto_time
Where
    X001ad_auto_time."ACTION" = "FINDING:"