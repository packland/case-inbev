# case-inbev

para iniciar, execute o comando do docker no diretorio dele

docker-compose --env-file ../.env  up 

crie uma imagem do spark

docker build -t delta-spark -f Dockerfile.spark .