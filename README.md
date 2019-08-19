# python-cloudamqp-elephantsql

Create a CloudAMQP instance and add the connection URL into the environmentvariable CLOUDAMQP_URL.
Create a ElephanSQL instance and add the connection URL into DATABASE_URL.

The app publish a message to RabbitMQ, subscribes this message and stores it into a PostgreSQL database.
