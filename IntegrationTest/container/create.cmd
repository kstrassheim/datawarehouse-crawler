@ECHO OFF
SETLOCAL
SET containersrc=mcr.microsoft.com/mssql/server:2017-latest
SET containertemppath=/tmp/
SET containersqlcmdpath=/opt/mssql-tools/bin/
SET containersqldatapath=/var/opt/mssql/data/

SET company=kstrassheim
SET name=mssql-server-linux-adventureworks
SET sa_pw="H5m4+?Ebbh*154"
SET port=9433
SET dbname=AdventureWorks2017
SET newdbname=Empty

(
	echo FROM %containersrc%
	echo COPY ./%dbname%.bak  %containertemppath%
)	> dockerfile

docker rm -f %name%
docker build -t %company%/%name% .
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=%sa_pw%" -d -p %port%:1433 --name %name% "%company%/%name%"

docker exec -it %name% %containersqlcmdpath%sqlcmd -S localhost -U SA -P %sa_pw% -Q "RESTORE Database [%dbname%] FROM DISK ='%containertemppath%%dbname%.bak' WITH MOVE '%dbname%' TO '%containersqldatapath%%dbname%.mdf', MOVE '%dbname%_log' TO '%containersqldatapath%%dbname%_log.ldf';CREATE DATABASE [%newdbname%]  ON  ( NAME = %newdbname%_dat, FILENAME = '%containersqldatapath%%newdbname%.mdf', SIZE = 10, FILEGROWTH = 5 ) LOG ON ( NAME = %newdbname%_log, FILENAME = '%containersqldatapath%%newdbname%_log.ldf', SIZE = 5, FILEGROWTH = 5 );ALTER DATABASE [%newdbname%] SET RECOVERY SIMPLE;BACKUP DATABASE [%dbname%] TO DISK = '%containertemppath%%dbname%.bak';BACKUP DATABASE [%newdbname%] TO DISK = '%containertemppath%%newdbname%.bak';"

docker commit %name% "%company%/%name%:latest"
docker save -o %cd%\%name% "%company%/%name%:latest"
del /f dockerfile
ENDLOCAL