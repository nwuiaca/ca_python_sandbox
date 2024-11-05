@echo off 
 
for /f "tokens=1 delims=" %%# in ('qprocess^|find /i /c /n "MicrosoftEdg"') do ( 
    set count=%%# 
) 
 
taskkill /F /IM MicrosoftEdge.exe /T
