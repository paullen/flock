echo "Running Client"
cd cmd/client
kill $(lsof -t -i:8082)
GO111MODULE=on go build
nohup command & ./client &

echo "Running Server"
cd ../server
GO111MODULE=on go build
kill $(lsof -t -i:8081)
nohup command & ./server &

