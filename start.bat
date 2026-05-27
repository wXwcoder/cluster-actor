
@echo on

SETLOCAL ENABLEDELAYEDEXPANSION

set Debug="%1"
echo %Debug%

set BIN_PATH=.\output\bin


wmic process where (name="consul.exe") get ProcessId | find /i "ProcessId" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo "result:"%ERRORLEVEL%
    start "consul" /min %BIN_PATH%\consul.exe agent -dev -ui -client 0.0.0.0
    ping 127.0.0.1 -n 3
)


start "node-1" go run .\main.go -config configs\config1.yaml
start "node-2" go run .\main.go -config configs\config2.yaml
