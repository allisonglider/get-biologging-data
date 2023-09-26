require(DBI)
require(duckdb)
require(dplyr)
require(arrow)
require(config)

db_loc <- 'raw_data/biologging-db.duckdb'

if (dir.exists('raw_data') == F) {dir.create('raw_data')}

aws.s3::save_object(object = "biologging-db.duckdb",
                    bucket = 's3://arcticecology-biologging',
                    file = db_loc, overwrite = T)

con <- duckdb::dbConnect(duckdb::duckdb(), dbdir = db_loc, read_only = TRUE)

gps <- arrow::open_dataset('s3://arcticecology-biologging/gps')
tdr <- arrow::open_dataset('s3://arcticecology-biologging/tdr')
acc <- arrow::open_dataset('s3://arcticecology-biologging/acc')
