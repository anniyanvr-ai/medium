@echo off
rem ---------------------------------------------------------------------------
rem  Runs the s3mover integration scenarios against the LOCAL MinIO (:9000) and
rem  SeaweedFS (:8333). Output: integration\real-run.log and
rem  integration\results\real-<timestamp>\REPORT.md (+ dashboard screenshots).
rem  Creates test buckets named s3mover-it-* in both servers.
rem ---------------------------------------------------------------------------
setlocal
cd /d "%~dp0"
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
set NO_PROXY=localhost,127.0.0.1
set LOG=%~dp0integration\real-run.log
echo started %date% %time% > "%LOG%"

set PY=
where py >nul 2>&1 && set PY=py -3
if not defined PY ( where python >nul 2>&1 && set PY=python )
if not defined PY ( echo ERROR: Python 3 not found on PATH >> "%LOG%" & goto :end )
%PY% --version >> "%LOG%" 2>&1

echo === pip install >> "%LOG%"
%PY% -m pip install --disable-pip-version-check -q --upgrade pip
%PY% -m pip install --disable-pip-version-check -q --prefer-binary -r requirements.txt pytest >> "%LOG%" 2>&1
rem optional extras: peak-memory numbers (psutil) and dashboard screenshots (playwright);
rem the run still works if they cannot be installed on this Python version
%PY% -m pip install --disable-pip-version-check -q --only-binary=:all: psutil >> "%LOG%" 2>&1
%PY% -m pip install --disable-pip-version-check -q --only-binary=:all: playwright >> "%LOG%" 2>&1

echo === unit tests >> "%LOG%"
%PY% -m pytest tests/unit -q -p no:cacheprovider >> "%LOG%" 2>&1

echo === scenarios (real MinIO + real SeaweedFS) >> "%LOG%"
%PY% integration\run_scenarios.py --source real ^
  --minio http://localhost:9000 --minio-key minioadmin --minio-secret minioadmin ^
  --seaweed http://localhost:8333 --seaweed-key seaweedadmin --seaweed-secret seaweedadmin ^
  --small 2000 --large 4 --large-mb 24 >> "%LOG%" 2>&1
echo scenarios exit=%errorlevel% >> "%LOG%"

:end
echo finished %date% %time% >> "%LOG%"
endlocal
