@echo off
echo === 1. Entorno de Python ===
python -m venv venv
call venv\Scripts\activate.bat
pip install -q -r requirements.txt

echo === 2. Infraestructura (Docker) ===
docker compose up -d --build --wait || goto :error

echo === 3. Carga historica directa a HBase ===
python carga_historica.py || goto :error

echo === 4. Camino en vivo ===
python main_pipeline.py || goto :error

echo === 5. Verificacion ===
python verificar.py || goto :error

echo === 6. Realizando consultas ===
echo === P1 a P3 ===
python p1_p3.py > resultados_p1_p3.txt 2>&1
type resultados_p1_p3.txt
if %errorlevel% neq 0 goto :error

echo === P4 ===
python p4.py > resultados_p4.txt 2>&1
type resultados_p4.txt
if %errorlevel% neq 0 goto :error

echo === P5 ===
python p5.py > resultados_p5.txt 2>&1
type resultados_p5.txt
if %errorlevel% neq 0 goto :error

echo === P6 ===
python p6.py > resultados_p6.txt 2>&1
type resultados_p6.txt
if %errorlevel% neq 0 goto :error

echo === D1 ===
python d1.py > resultados_d1.txt 2>&1
type resultados_d1.txt
if %errorlevel% neq 0 goto :error

echo === D2 ===
python d2.py > resultados_d2.txt 2>&1
type resultados_d2.txt
if %errorlevel% neq 0 goto :error

python main_pipeline.py

echo === Listo ===
goto :fin


:error
echo.
echo [ERROR] Ocurrio un fallo en el proceso.

:fin
echo.
pause