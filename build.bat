@echo off
chcp 65001 >nul
echo ========================================
echo   Network Chat - 局域网聊天工具
echo   编译脚本
echo ========================================
echo.

REM Check Go
where go >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [错误] 未找到 Go，请先安装: https://golang.google.cn/dl/
    pause
    exit /b 1
)

echo [1/3] 下载依赖...
go mod tidy
if %ERRORLEVEL% NEQ 0 (
    echo [错误] 依赖下载失败，尝试国内代理...
    set GOPROXY=https://goproxy.cn,direct
    go mod tidy
)

echo.
echo [2/3] 编译...
go build -ldflags="-s -w" -o network_chat.exe .
if %ERRORLEVEL% NEQ 0 (
    echo [错误] 编译失败
    pause
    exit /b 1
)

echo.
echo [3/3] 完成！
echo.
echo 可执行文件: network_chat.exe
echo.
echo 使用方法:
echo   network_chat.exe -n 你的名字
echo   network_chat.exe -n 你的名字 -p 8765
echo.
echo 发给同事只需这一个 .exe 文件即可！
echo ========================================
pause
