Select
    X001ax_Deferments_final_curr.DEFER_TYPE_DESC,
    Count(X001ax_Deferments_final_curr.STUDENT_VSS) As Count_STUDENT_VSS
From
    X001ax_Deferments_final_curr
Group By
    X001ax_Deferments_final_curr.DEFER_TYPE_DESC