
rm ../dask_lambda.zip
zip -r ../dask_lambda.zip . 
aws s3 cp ../dask_lambda.zip s3://mikeokslonger-dask/dask_lambda.zip
