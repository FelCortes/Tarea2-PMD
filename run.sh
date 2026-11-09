#!/bin/bash
# Reproduce todo de punta a punta (Linux/Mac). En Windows: run.bat
set -e
cd "$(dirname "$0")"

echo "=== 1. Entorno de Python ==="
python3 -m venv venv
source venv/bin/activate
pip install -q -r requirements.txt

echo "=== 2. Infraestructura (Docker) ==="
docker compose up -d --build --wait

echo "=== 3. Carga historica directa a HBase ==="
python carga_historica.py

echo "=== 4. Camino en vivo ==="
python main_pipeline.py

echo "=== 5. Verificacion ==="
python verificar.py

echo "=== 6. Realizando consultas ==="
echo "=== P1 a P3 ==="
python p1_p3.py 2>&1 | tee resultados_p1_p3.txt

echo "=== P4 ==="
python p4.py 2>&1 | tee resultados_p4.txt

echo "=== P5 ==="
python p5.py 2>&1 | tee resultados_p5.txt

echo "=== P6 ==="
python p6.py 2>&1 | tee resultados_p6.txt

echo "=== D1 ==="
python d1.py 2>&1 | tee resultados_d1.txt

echo "=== D2 ==="
python d2.py 2>&1 | tee resultados_d2.txt

python main_pipeline.py

echo "=== Listo ==="