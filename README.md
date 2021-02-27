Swaptacular service that manages creditors
==========================================

This service implements a
[Swaptacular](https://github.com/epandurski/swaptacular) [messaging
protocol](https://github.com/epandurski/swpt_accounts/blob/master/protocol.rst)
client that manages creditors. The deliverables are two docker images:
the app-image, and the swagger-ui-image. Both images are generated
from the project's
[Dockerfile](https://github.com/epandurski/swpt_creditors/blob/master/Dockerfile). The
app-image is the creditor managing service. The swagger-ui-image is a
simple Swagger UI cleint for the service. To find out what processes
can be spawned from the generated app-image, see the
[entrypoint](https://github.com/epandurski/swpt_creditors/blob/master/docker/entrypoint.sh). For
the available configuration options, see the
[development.env](https://github.com/epandurski/swpt_creditors/blob/master/development.env)
file.  This
[example](https://github.com/epandurski/swpt_creditors/blob/master/docker-compose-all.yml)
shows how to use the generated image.


How to run the unit tests
-------------------------

1.  Install [Docker](https://docs.docker.com/) and [Docker
    Compose](https://docs.docker.com/compose/).

2.  To create an *.env* file with reasonable defalut values, use this
    command:

        $ cp development.env .env

3.  To run the unit tests, use the following commands:

        $ docker-compose build
        $ docker-compose run tests-config test

4.  To run the minimal set of services needed for development, use
    this command:

        $ docker-compose up --build


How to setup a development environment
--------------------------------------

1.  Install [Poetry](https://poetry.eustace.io/docs/).

2.  Create a new [Python](https://docs.python.org/) virtual
    environment and activate it.

3.  To install dependencies, use this command:

        $ poetry install

4.  You can use `flask run -p 5000` to run a local web server,
    `dramatiq tasks:protocol_broker` to spawn local task workers, and
    `pytest --cov=swpt_creditors --cov-report=html` to run the tests
    and generate a test coverage report.


How to run all services (production-like)
-----------------------------------------

To start the containers, use this command:

    $ docker-compose -f docker-compose-all.yml up --build