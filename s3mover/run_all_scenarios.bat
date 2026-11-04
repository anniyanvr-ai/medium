@echo off
rem =====================================================================
rem  s3mover - run all 6 scenarios (no Docker needed)
rem    MinIO  : http://localhost:9000   minioadmin / minioadmin
rem    SeaweedFS S3: http://localhost:8333  (any key if S3 auth is off)
rem  Edit the 3 lines below, then double-click this file.
rem  Dashboard while it runs: http://127.0.0.1:8765
rem =====================================================================
setlocal
set SRC_BUCKET=rts-epcis
set FROM=2025-06-01
set TO=2025-08-31

cd /d "%~dp0"
set PYTHONUTF8=1
set NO_PROXY=localhost,127.0.0.1
set PY=py -3
where py >nul 2>&1 || set PY=python
%PY% -m pip install --disable-pip-version-check -q --upgrade pip
%PY% -m pip install --disable-pip-version-check -q --prefer-binary -r requirements.txt

set MM=configs\minio-to-minio.yaml
set SW=configs\minio-to-seaweedfs.yaml
set SUMMARY=state\scenario-summary.txt
if not exist state mkdir state
echo s3mover scenarios %date% %time% > %SUMMARY%

call :run "1 MinIO-MinIO     full"    %MM% mm-full    %SRC_BUCKET%-full    --mode full
call :run "2 MinIO-MinIO     since"   %MM% mm-since   %SRC_BUCKET%-since   --mode since --from %FROM%
call :run "3 MinIO-MinIO     between" %MM% mm-between %SRC_BUCKET%-between --mode between --from %FROM% --to %TO%
call :run "4 MinIO-SeaweedFS full"    %SW% sw-full    %SRC_BUCKET%-full    --mode full
call :run "5 MinIO-SeaweedFS since"   %SW% sw-since   %SRC_BUCKET%-since   --mode since --from %FROM%
call :run "6 MinIO-SeaweedFS between" %SW% sw-between %SRC_BUCKET%-between --mode between --from %FROM% --to %TO%

type %SUMMARY%
echo.
echo Dashboard for all jobs: %PY% -m s3mover dashboard --state-dir state   then open http://127.0.0.1:8765
pause
goto :eof

:run
rem %1 label  %2 config  %3 job  %4 dest bucket  %5.. mode args
set LABEL=%~1
set CFG=%2
set JOB=%3
set DST=%4
shift & shift & shift & shift
echo.
echo ===== %LABEL%  (job %JOB%, %SRC_BUCKET% -^> %DST%) =====
%PY% -m s3mover run -c %CFG% --job-name %JOB% --src-bucket %SRC_BUCKET% --dest-bucket %DST% %1 %2 %3 %4 %5 %6
set RC=%errorlevel%
if "%RC%"=="0" (echo %LABEL% : COMPLETE, verified 100%%  [job %JOB%] >> %SUMMARY%) else (echo %LABEL% : exit %RC% - run again to resume  [job %JOB%] >> %SUMMARY%)
goto :eof
