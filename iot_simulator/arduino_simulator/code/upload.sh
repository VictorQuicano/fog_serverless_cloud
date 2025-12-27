#!/bin/bash
# build_and_push_fixed.sh

# Variables configurables
PROJECT_ID="arduino-tesis"
IMAGE_NAME="iot-sensor-simulator"
TAG="latest"  # Cambiar a v1.0 para coincidir
REGION="us-central1"
REPOSITORY="iot-sensor-simulator"

# 1. Autenticarse con Google Cloud
echo "Autenticando con Google Cloud..."
# gcloud auth login
gcloud config set project $PROJECT_ID

# 2. Habilitar APIs necesarias (agregar Artifact Registry)
echo "Habilitando APIs necesarias..."
gcloud services enable artifactregistry.googleapis.com
gcloud services enable run.googleapis.com  # Para Cloud Run
gcloud services enable pubsub.googleapis.com
gcloud services enable storage.googleapis.com

# 3. Crear repositorio en Artifact Registry si no existe
echo "Verificando/Creando repositorio en Artifact Registry..."
if ! gcloud artifacts repositories describe $REPOSITORY --location=$REGION &> /dev/null; then
    gcloud artifacts repositories create $REPOSITORY \
        --repository-format=docker \
        --location=$REGION \
        --description="IoT Sensor Simulator"
fi

# 4. Configurar Docker para Artifact Registry
echo "Configurando Docker para Artifact Registry..."
gcloud auth configure-docker $REGION-docker.pkg.dev

# 5. Construir la imagen Docker
echo "Construyendo imagen Docker..."
docker build -t $IMAGE_NAME:$TAG .

# 6. Etiquetar la imagen para Artifact Registry
echo "Etiquetando imagen para Artifact Registry..."
docker tag $IMAGE_NAME:$TAG $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/$IMAGE_NAME:$TAG

# 7. Subir la imagen a Artifact Registry
echo "Subiendo imagen a Artifact Registry..."
docker push $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/$IMAGE_NAME:$TAG

echo "Imagen subida exitosamente a: $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/$IMAGE_NAME:$TAG"

# 8. También subir a GCR por compatibilidad (opcional)
echo "¿Deseas también subir a GCR? (y/n)"
read -r response
if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
    gcloud auth configure-docker gcr.io
    docker tag $IMAGE_NAME:$TAG gcr.io/$PROJECT_ID/$IMAGE_NAME:$TAG
    docker push gcr.io/$PROJECT_ID/$IMAGE_NAME:$TAG
    echo "Imagen también subida a GCR: gcr.io/$PROJECT_ID/$IMAGE_NAME:$TAG"
fi