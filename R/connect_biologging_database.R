require(DBI)
require(duckdb)
require(dplyr)
require(arrow)
require(config)
require(dbplyr)

Sys.setenv('AWS_ACCESS_KEY_ID' = config::get()$aws_key,
           'AWS_SECRET_ACCESS_KEY' = config::get()$aws_secret,
           'AWS_DEFAULT_REGION' = config::get()$aws_region)

db_loc <- 'raw_data/biologging-db.duckdb'

if (dir.exists('raw_data') == F) {dir.create('raw_data')}

aws.s3::save_object(object = "biologging-db.duckdb",
                    bucket = 's3://arcticecology-biologging',
                    file = db_loc, overwrite = T)

con <- duckdb::dbConnect(duckdb::duckdb(), dbdir = db_loc, read_only = T)

gps <- arrow::open_dataset('s3://arcticecology-biologging/gps')
tdr <- arrow::open_dataset('s3://arcticecology-biologging/tdr')
acc <- arrow::open_dataset('s3://arcticecology-biologging/acc')
