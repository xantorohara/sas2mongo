set MONGO_DB_PATH=mongo-db

md %MONGO_DB_PATH%

start mongod --dbpath=%MONGO_DB_PATH%