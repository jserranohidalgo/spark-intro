[comment]: # (Start Badges)

[![Gitter](https://badges.gitter.im/tfgs-functionalprogramming-urjc/spark-tfgs.svg)](https://gitter.im/tfgs-functionalprogramming-urjc/spark-tfgs?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

[comment]: # (End Badges)


# A crash course on Spark in Scala

## Content

The course is divided into four major modules.

1. The language of Datasets
2. The language of DataFrames
3. Spark on AWS
4. Spark optmizations & good practices

## Chat room

Join the chat room at: https://gitter.im/tfgs-functionalprogramming-urjc/spark-tfgs#share

## Set up using docker

The course is run on Jupyter notebooks. We recommend installing docker to run jupyter with Spark. Please, follow these steps to set up everything you need.

1. Install docker on your machine

    1.1 [Linux](https://docs.docker.com/install/linux/docker-ce/ubuntu/)

    1.2 [Windows](https://docs.docker.com/docker-for-windows/install/)
        (if your windows is not 10 pro, install [this one](https://docs.docker.com/toolbox/toolbox_install_windows/))

    1.3 [MAC](https://docs.docker.com/docker-for-mac/install/)

2. Clone this repository

    2.1 Open a terminal

    2.2 Go to the folder where you want to download this repo

    2.3 Run this command:
    ```bash
    git clone https://github.com/jserranohidalgo/spark-intro.git [<nombre del proyecto>]
    ```

    2.4 Move into your local repository
    ```bash
    cd <nombre del proyecto>
    ```
3. Run the docker image for Spark

    3.1 Make sure that docker daemon is running

    3.2 Run the following command:

    - Linux / MAC

        ```bash
        docker run -it --rm -p 8888:8888 -p 4040:4040 -m 4g -v "$PWD":/home/jovyan/work almondsh/almond:0.9.1
        ```

    - Windows

        ```bash
        docker run -it --rm -p 8888:8888 -p 4040:4040 -m 4g -v {c:/path/to/downloaded/folder}:/home/jovyan/work almondsh/almond:0.9.1
        ```

    It can take a while to download the image for the first time.

4. Enter Jupyter

    4.1 Copy the token that is shown in the terminal
    ```
    Copy/paste this URL into your browser when you connect for the first time, to login with a token:
    http://(824918044eeb or 127.0.0.1):8888/?token=57c1a89124a898d1c6d4ca404445c9b54c9c6d6cfc558f9f
    ```

    4.2 Go to [localhost](http://localhost:8888), paste the token and log in.
    ![Jupyter token login](images/jupyter-token.png)

5. Test Notebook

    5.1 In Jupyter open with a click the work folder

    5.2 Open with a click the `Test notebook`

    5.3 Press the run button, if yo don't see any errors and at the end you see `it works!` you have all up and running.

    ![ok-result](images/ok-result.png)


6. Close the jupyter container

    ```
    Atention!
    ```

    Make sure that you save everything before closing Jupyter. The docker parameter `--rm`- deletes the container (the image is still in your machine), so as to get a clean enviroment each time you open it, but also discards all non saved changes in the notebooks.

    6.1 Go to the shell that is running the container

    6.2 Control + c to send a kill signal

    6.3 Type 'y' and enter to finish.

## Standalone set up

You can install jupyter and the Scala kernel yourself. Please follow the following instructions:
- [Jupyter installation](https://jupyter.org/install)
- [Scala kernel installation](https://almond.sh/docs/quick-start-install)


## Acknowledgements

This course owes a lot to the staff of [Habla Computing](https://hablapps.com). Particular thanks to Mikel San Vicente and Alfonso Roa.


