docker run --name elyra -it -p 8888:8888 -p 45859:45859 -v C:/Code/Elyra/:/home/jovyan/work -w /home/jovyan/work elyra/elyra:1.2.1 jupyter lab --debug


docker run --name smartnoise -it -p 8888:8888 -v C:/Temp/smartnoise/:/home/jovyan/work -w /home/jovyan/work elyra/elyra:1.2.1 jupyter lab