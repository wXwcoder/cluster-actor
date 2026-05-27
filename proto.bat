@echo off
setlocal

set PROTO_DIR=proto
set OUTPUT_DIR=gen

echo Generating Go code from proto files...

mkdir %OUTPUT_DIR% 2>nul

protoc --go_out=%OUTPUT_DIR% --go_opt=M%PROTO_DIR%/base.proto=. %PROTO_DIR%/base.proto
protoc --go_out=%OUTPUT_DIR% --go_opt=M%PROTO_DIR%/user.proto=. %PROTO_DIR%/user.proto

if %errorlevel% equ 0 (
    echo Successfully generated Go code to %OUTPUT_DIR%
) else (
    echo Error: Failed to generate Go code from proto files
    exit /b 1
)

endlocal