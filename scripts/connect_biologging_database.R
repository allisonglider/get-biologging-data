require(DBI)
require(RPostgreSQL)
require(RPostgres)
require(arrow)
require(config)

con <- DBI::dbConnect(RPostgres::Postgres(), 
                      host = config::get()$host_db, 
                      dbname = config::get()$db, 
                      port = config::get()$db_port, 
                      user = config::get()$db_user, 
                      password = config::get()$db_password)

Sys.setenv('AWS_ACCESS_KEY_ID' = config::get()$aws_key,
           'AWS_SECRET_ACCESS_KEY' = config::get()$aws_secret,
           'AWS_DEFAULT_REGION' = config::get()$aws_region)

gps <- arrow::open_dataset('s3://arcticecology-biologging/gps')
tdr <- arrow::open_dataset('s3://arcticecology-biologging/tdr')
acc <- arrow::open_dataset('s3://arcticecology-biologging/acc')
