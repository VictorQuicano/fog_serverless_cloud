from main import app, main

# Iniciar la simulación al cargar la aplicación con Gunicorn
# main() lee variables de entorno y lanza el hilo de simulación
print("Inicializando aplicación y simulación...")
main()

if __name__ == "__main__":
    app.run()