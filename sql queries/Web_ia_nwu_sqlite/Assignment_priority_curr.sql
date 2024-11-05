Select
    assc.Priority_number,
    assc.Priority_word,
    Count(assc.File) As Count_File
From
    X000_Assignment_curr assc
Group By
    assc.Priority_number,
    assc.Priority_word