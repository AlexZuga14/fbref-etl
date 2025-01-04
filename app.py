from flask import Flask, request, send_file, render_template, jsonify
from etl import process_fbref_data  # Importa tu script ETL
import os

app = Flask(__name__)

# Ruta para la página principal (frontal)
@app.route('/')
def home():
    return render_template('index.html')  # Usa render_template para servir el HTML

# Ruta para procesar los datos enviados desde el formulario
@app.route('/procesar', methods=['POST'])
def procesar():
    try:
        # Obtener datos del formulario
        url = request.form.get('urlInput')
        team_type = request.form.get('teamType')

        if not url or not team_type:
            return jsonify({"error": "Faltan datos requeridos"}), 400

        # Procesar los datos con el script ETL
        df = process_fbref_data(url, tipo=team_type)

        # Nombre del archivo generado por el script
        base_name = url.split("/")[-1]
        file_name = f"{team_type}_{base_name}.csv"

        # Verificar si el archivo existe antes de enviarlo
        if not os.path.exists(file_name):
            return jsonify({"error": "Archivo CSV no encontrado"}), 500

        # Devolver el archivo CSV al cliente
        return send_file(file_name, as_attachment=True)

    except Exception as e:
        return jsonify({"error": f"Ocurrió un error: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
