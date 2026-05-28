@echo off
setlocal

set PROTO_DIR=proto
set OUTPUT_DIR=gen

echo Generating Go code from proto files...

mkdir %OUTPUT_DIR% 2>nul

protoc --proto_path=%PROTO_DIR% --go_out=%OUTPUT_DIR% --go_opt=paths=source_relative %PROTO_DIR%/error.proto
protoc --proto_path=%PROTO_DIR% --go_out=%OUTPUT_DIR% --go_opt=paths=source_relative %PROTO_DIR%/base.proto
protoc --proto_path=%PROTO_DIR% --go_out=%OUTPUT_DIR% --go_opt=paths=source_relative %PROTO_DIR%/user.proto
protoc --proto_path=%PROTO_DIR% --go_out=%OUTPUT_DIR% --go_opt=paths=source_relative %PROTO_DIR%/chat.proto
protoc --proto_path=%PROTO_DIR% --go_out=%OUTPUT_DIR% --go_opt=paths=source_relative %PROTO_DIR%/msgid.proto

if %errorlevel% equ 0 (
    echo Successfully generated Go code to %OUTPUT_DIR%
) else (
    echo Error: Failed to generate Go code from proto files
    exit /b 1
)

endlocal